
"""
paper_pipeline.py — Apache Airflow DAG for INFO-H420 Assignment 3.

Turns research PDFs into a searchable study library:

    scan -> convert -> summarize -> store -> build_html -> notify

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
import logging

log = logging.getLogger(__name__)

@dag(
    dag_id="paper_pipeline",
    description="Convert research PDFs into a searchable study library",
    schedule=timedelta(minutes=2),      # scheduled DAG that looks for new PDFs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                   # avoid two runs touching the DB at once
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["info-h420", "llm", "rag"],
)
def paper_pipeline():

    @task
    def scan() -> list[str]:
        """Return PDF filenames present in papers/ but not yet in the database."""
        new_files = paper_lib.scan_for_new_pdfs()
        log.info("[scan] %d new PDF(s): %s", len(new_files), new_files)
        return new_files

    @task
    def convert(filename: str) -> dict[str, str]:
        """Convert new PDF to Markdown. Returns [{filename, md_path}, ...]."""
        md_path = paper_lib.pdf_to_markdown(filename)
        log.info("[convert] %s -> %s", filename, md_path)
        return {"filename": filename, "md_path": md_path}
    
    @task
    def summarize(item: dict[str, str]) -> dict:
        """Ask the local LLM for title/summary/keywords/category for each paper."""
        meta = paper_lib.summarize_markdown(item["filename"], item["md_path"])
        log.info("[summarize] %s -> %s", meta["filename"], meta)
        return meta

    @task(max_active_tis_per_dagrun=1)  # avoid concurrent SQLite writes
    def store(meta: dict) -> dict:
        """Write one row per paper to SQLite."""
        paper_lib.save_paper(meta)
        log.info("[store] saved %s", meta["filename"])
        return meta
    
    @task
    def build_html() -> str:
        """
        Regenerate the static index.html listing all papers.
        The parameter is not used, but only present to create a dependency on the store() tasks.
        """
        out = paper_lib.generate_html()
        log.info("[html] wrote %s", out)
        return out
    
    @task
    def notify(metas: list[dict]) -> None:
        """Send one Discord summary (split into chunks if long) for this run's papers."""
        sent = paper_lib.send_discord_notification(list(metas))
        log.info("[notify] %d message(s) sent for %d paper(s)", sent, len(metas))

    

    # ----- Task wiring ------------------------------------------------------
    papers = scan()
    converted = convert.expand(filename=papers)
    metadata = summarize.expand(item=converted)
    stored = store.expand(meta=metadata)
    stored >> build_html() >> notify(metadata)
    
paper_pipeline()
