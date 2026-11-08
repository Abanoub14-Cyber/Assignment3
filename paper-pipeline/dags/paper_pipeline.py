"""
paper_pipeline.py — Apache Airflow DAG for INFO-H420 Assignment 3.

Turns research PDFs into a searchable study library:

    init_db -> scan -> convert -> summarize -> store -> build_html -> notify

All the real logic lives in paper_lib.py; this file only wires the tasks.
"""

from __future__ import annotations

from datetime import datetime, timedelta

# Robust across Airflow 3 (airflow.sdk) and Airflow 2 (airflow.decorators).
try:
    from airflow.sdk import dag, task
except ImportError:  # pragma: no cover
    from airflow.decorators import dag, task

import paper_lib


@dag(
    dag_id="paper_pipeline",
    description="Convert research PDFs into a searchable study library",
    schedule=timedelta(minutes=10),      # scheduled DAG that looks for new PDFs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                   # avoid two runs touching the DB at once
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["info-h420", "llm", "rag"],
)
def paper_pipeline():

    @task
    def init_db() -> None:
        """Create the SQLite table if it does not exist yet."""
        paper_lib.init_db()

    @task
    def scan() -> list[str]:
        """Return PDF filenames present in papers/ but not yet in the database."""
        new_files = paper_lib.scan_for_new_pdfs()
        print(f"[scan] {len(new_files)} new PDF(s): {new_files}")
        return new_files

    @task
    def convert(new_files: list[str]) -> list[dict]:
        """Convert each new PDF to Markdown. Returns [{filename, md_path}, ...]."""
        results = []
        for filename in new_files:
            md_path = paper_lib.pdf_to_markdown(filename)
            print(f"[convert] {filename} -> {md_path}")
            results.append({"filename": filename, "md_path": md_path})
        return results

    @task
    def summarize(converted: list[dict]) -> list[dict]:
        """Ask the local LLM for title/summary/keywords/category for each paper."""
        metas = []
        for item in converted:
            meta = paper_lib.summarize_markdown(item["filename"], item["md_path"])
            print(f"[summarize] {meta['filename']} -> {meta['title']!r}")
            metas.append(meta)
        return metas

    @task
    def store(metas: list[dict]) -> list[dict]:
        """Write one row per paper to SQLite."""
        for meta in metas:
            paper_lib.save_paper(meta)
            print(f"[store] saved {meta['filename']}")
        return metas

    @task
    def build_html(metas: list[dict]) -> str:
        """Regenerate the static index.html listing all papers."""
        out = paper_lib.generate_html()
        print(f"[html] wrote {out}")
        return out

    @task(trigger_rule="all_done")
    def notify(metas: list[dict]) -> None:
        """Send a Discord webhook for each newly processed paper."""
        for meta in metas or []:
            sent = paper_lib.send_discord_notification(meta)
            print(f"[notify] {meta['filename']}: {'sent' if sent else 'skipped'}")

    # ----- task dependencies (this is the DAG graph) ----------------------- #
    new_files = scan()
    converted = convert(new_files)
    metas = summarize(converted)
    stored = store(metas)
    html = build_html(stored)

    init_db() >> new_files          # make sure the table exists before scanning
    html >> notify(stored)          # notify only after the site is rebuilt


paper_pipeline()
