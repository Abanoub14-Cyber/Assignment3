"""
paper_lib.py
============
Core logic for the research-paper pipeline (INFO-H420, Assignment 3).

This module has no Apache Airflow imports, so each function can be reused and
tested on its own; the Airflow DAG in paper_pipeline.py only orchestrates them.

Pipeline steps implemented here:
  1. init_db / already_processed / scan_for_new_pdfs  -> find new PDFs
  2. pdf_to_markdown                                   -> convert a PDF to text
  3. summarize_markdown                                -> call the local LLM
  4. save_paper                                        -> write one row to SQLite
  5. generate_html                                     -> rebuild the static site
  6. send_discord_notification                         -> ping a Discord channel
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from html import escape
from pathlib import Path

import pymupdf4llm                 # PDF -> Markdown
import requests                    # Discord webhook
from openai import OpenAI          # OpenAI-compatible client for the local LLM


# --------------------------------------------------------------------------- #
# Configuration (read from environment variables; see .env.example)           #
# --------------------------------------------------------------------------- #
# PAPERS_DIR and OUTPUT_DIR live INSIDE the Google Drive synced folder.
# DB_PATH lives OUTSIDE it, on the local machine only.

def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


PAPERS_DIR = Path(_env("PAPERS_DIR", "./ResearchLibrary/papers")).expanduser()
OUTPUT_DIR = Path(_env("OUTPUT_DIR", "./ResearchLibrary/output")).expanduser()
DB_PATH = Path(_env("DB_PATH", "./data/papers.db")).expanduser()
MARKDOWN_DIR = Path(_env("MARKDOWN_DIR", "./data/markdown")).expanduser()

# Local LLM served by LM Studio (default endpoint :1234)
LLM_BASE_URL = _env("LLM_BASE_URL", "http://localhost:1234/v1")
LLM_API_KEY = _env("LLM_API_KEY", "not-needed")      # local servers ignore it
LLM_MODEL = _env("LLM_MODEL", "model")               # LM Studio accepts "model"
LLM_MAX_CHARS = int(_env("LLM_MAX_CHARS", "12000"))  # trim long papers to fit

# Discord webhook (optional: if unset, the notify step is skipped gracefully)
DISCORD_WEBHOOK_URL = _env("DISCORD_WEBHOOK_URL", "")

# The HTML template lives next to this file.
TEMPLATE_PATH = Path(__file__).resolve().parent / "template.html"


# --------------------------------------------------------------------------- #
# Database                                                                    #
# --------------------------------------------------------------------------- #
def init_db(db_path: Path = DB_PATH) -> None:
    """Create the papers table if it does not exist. Safe to call every run."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                filename   TEXT PRIMARY KEY,   -- unique, as per the assignment
                title      TEXT NOT NULL,
                summary    TEXT NOT NULL,
                keywords   TEXT NOT NULL,       -- stored as a JSON string
                category   TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def already_processed(db_path: Path = DB_PATH) -> set[str]:
    """Return the set of filenames already in the database."""
    if not Path(db_path).exists():
        return set()
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT filename FROM papers").fetchall()
    return {r[0] for r in rows}


def scan_for_new_pdfs(
    papers_dir: Path = PAPERS_DIR, db_path: Path = DB_PATH
) -> list[str]:
    """List PDF filenames that exist in papers_dir but not yet in the DB."""
    papers_dir = Path(papers_dir)
    if not papers_dir.exists():
        return []
    done = already_processed(db_path)
    found = sorted(p.name for p in papers_dir.glob("*.pdf"))
    return [name for name in found if name not in done]


def save_paper(meta: dict, db_path: Path = DB_PATH) -> None:
    """Insert (or replace) one paper row. `keywords` is serialised to JSON."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO papers
                (filename, title, summary, keywords, category, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                meta["filename"],
                meta["title"],
                meta["summary"],
                json.dumps(meta["keywords"], ensure_ascii=False),
                meta["category"],
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            ),
        )
        conn.commit()


def load_all_papers(db_path: Path = DB_PATH) -> list[dict]:
    """Return every paper as a dict, keywords parsed back into a list."""
    if not Path(db_path).exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM papers ORDER BY created_at DESC"
        ).fetchall()
    papers = []
    for r in rows:
        d = dict(r)
        try:
            d["keywords"] = json.loads(d["keywords"])
        except (json.JSONDecodeError, TypeError):
            d["keywords"] = []
        papers.append(d)
    return papers


# --------------------------------------------------------------------------- #
# PDF -> Markdown                                                             #
# --------------------------------------------------------------------------- #
def pdf_to_markdown(
    filename: str,
    papers_dir: Path = PAPERS_DIR,
    markdown_dir: Path = MARKDOWN_DIR,
) -> str:
    """Convert one PDF to Markdown with PyMuPDF4LLM; cache it; return the path."""
    markdown_dir = Path(markdown_dir)
    markdown_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = Path(papers_dir) / filename
    md_text = pymupdf4llm.to_markdown(str(pdf_path))

    md_path = markdown_dir / (Path(filename).stem + ".md")
    md_path.write_text(md_text, encoding="utf-8")
    return str(md_path)


# --------------------------------------------------------------------------- #
# LLM summarisation                                                           #
# --------------------------------------------------------------------------- #
SYSTEM_PROMPT = (
    "You are a research librarian. You read academic papers and extract "
    "structured metadata. You always respond with a single valid JSON object "
    "and nothing else."
)

USER_PROMPT_TEMPLATE = """\
Read the following research paper (it may be truncated) and return a JSON \
object with EXACTLY these four fields:

  "title":    the paper's title (string)
  "summary":  a concise 2-3 sentence summary (string)
  "keywords": 3 to 6 topic keywords (array of strings)
  "category": one broad area, e.g. "Machine Learning", "Databases", \
"Data Engineering", "Information Retrieval", "Natural Language Processing" \
(string)

Return ONLY the JSON object.

Paper:
\"\"\"
{content}
\"\"\"
"""


def _extract_json(raw: str) -> dict:
    """Parse JSON, falling back to the first {...} block if needed."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start : end + 1])
        raise


def _normalize(data: dict, filename: str) -> dict:
    """Guarantee the four fields exist with the right basic types."""
    title = str(data.get("title") or Path(filename).stem).strip()
    summary = str(data.get("summary") or "No summary produced.").strip()
    category = str(data.get("category") or "Uncategorized").strip()

    kw = data.get("keywords") or []
    if isinstance(kw, str):
        kw = [k.strip() for k in kw.split(",") if k.strip()]
    kw = [str(k).strip() for k in kw][:6]

    return {
        "filename": filename,
        "title": title,
        "summary": summary,
        "keywords": kw,
        "category": category,
    }


def summarize_markdown(
    filename: str,
    md_path: str,
    *,
    base_url: str = LLM_BASE_URL,
    api_key: str = LLM_API_KEY,
    model: str = LLM_MODEL,
    max_chars: int = LLM_MAX_CHARS,
) -> dict:
    """Send the paper text to the local LLM and return normalised metadata.

    Mirrors the practice-session notebook (OpenAI client -> local base_url).
    Retries once if the model returns something that is not valid JSON, as the
    assignment allows ("If parsing fails, retry once").
    """
    client = OpenAI(api_key=api_key, base_url=base_url)
    content = Path(md_path).read_text(encoding="utf-8")[:max_chars]
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT_TEMPLATE.format(content=content)},
    ]

    last_error: Exception | None = None
    for attempt in range(2):  # initial try + one retry
        try:
            # Ask for JSON mode; some servers may not support it, so fall back.
            kwargs = dict(model=model, messages=messages, temperature=0)
            if attempt == 0:
                kwargs["response_format"] = {"type": "json_object"}
            completion = client.chat.completions.create(**kwargs)
            raw = completion.choices[0].message.content
            return _normalize(_extract_json(raw), filename)
        except Exception as exc:  # retry once on any failure
            last_error = exc
            continue
    raise RuntimeError(
        f"LLM did not return valid JSON for {filename}: {last_error}"
    )


# --------------------------------------------------------------------------- #
# Static HTML generation (template lives in template.html)                    #
# --------------------------------------------------------------------------- #
def generate_html(output_dir: Path = OUTPUT_DIR, db_path: Path = DB_PATH) -> str:
    """Rebuild index.html from template.html, listing all papers in the DB."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    papers = load_all_papers(db_path)

    # Papers are embedded as JSON and filtered client-side (no server needed).
    data_json = json.dumps(papers, ensure_ascii=False)
    categories = sorted({p["category"] for p in papers})
    options = "\n".join(
        f'<option value="{escape(c)}">{escape(c)}</option>' for c in categories
    )
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = TEMPLATE_PATH.read_text(encoding="utf-8")
    html = (
        html.replace("__COUNT__", str(len(papers)))
        .replace("__GENERATED__", generated)
        .replace("__OPTIONS__", options)
        .replace("__DATA_JSON__", data_json)  # last: injected content is verbatim
    )

    out_path = output_dir / "index.html"
    out_path.write_text(html, encoding="utf-8")
    return str(out_path)


# --------------------------------------------------------------------------- #
# Discord notification                                                        #
# --------------------------------------------------------------------------- #
def send_discord_notification(
    meta: dict, webhook_url: str = DISCORD_WEBHOOK_URL
) -> bool:
    """Post a message to a Discord webhook. Returns True if sent."""
    if not webhook_url:
        return False  # not configured -> skip quietly

    kw = ", ".join(meta.get("keywords", []))
    content = (
        f"📄 **New paper processed**\n"
        f"**{meta['title']}**\n"
        f"_{meta['category']}_ · `{meta['filename']}`\n"
        f"{meta['summary']}\n"
        f"🔖 {kw}"
    )
    resp = requests.post(webhook_url, json={"content": content[:1900]}, timeout=15)
    resp.raise_for_status()
    return True
