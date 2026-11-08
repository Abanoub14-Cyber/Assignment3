# Research Paper Pipeline — INFO-H420 Assignment 3

A local data pipeline, orchestrated with **Apache Airflow**, that turns a folder
of research PDFs into a searchable study library. For every new PDF it converts
the file to Markdown, asks a **locally served LLM** (via LM Studio) for a title,
summary, keywords and category, stores that metadata in **SQLite**, regenerates a
static **HTML** page with search and category filtering, and sends a **Discord**
notification.

```
init_db → scan → convert → summarize → store → build_html → notify
```

## Project layout

```
paper-pipeline/
├── dags/
│   ├── paper_pipeline.py   # the Airflow DAG 
│   ├── paper_lib.py        # all the logic (DB, PDF, LLM, HTML, Discord)
│   └── template.html       # HTML template for the generated page
├── requirements.txt
├── .env.example            # copy to .env and fill in
├── .gitignore
└── README.md
```

PDFs and the generated index.html live in the Google Drive–synced folder; the
SQLite database is kept **outside** it (syncing a live database would corrupt it).

## Prerequisites

- Python 3.12
- [LM Studio](https://lmstudio.ai) (serves the local LLM)

## Setup and run

**1. Create a virtual environment and install the dependencies.**
Airflow must be installed pinned to its constraints file.
```bash
python3.12 -m venv .venv
source .venv/bin/activate

AIRFLOW_VERSION=3.0.2
PY=3.12
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
pip install -r requirements.txt
```

**2. Start the local LLM.**
In LM Studio: download **Gemma 3 1B Instruct**, open the *Developer* tab, load the
model, and **start the server** (it serves `http://localhost:1234/v1`).

**3. Configure.**
```bash
cp .env.example .env
```
Edit `.env`: set the Google Drive paths (here we did it with a local folder outside the project's folder, we didn't setup a real Google Drive) and paste your **Discord webhook URL**
(Discord → Edit Channel → Integrations → Webhooks → New Webhook → Copy URL).

**4. Run Airflow.**
```bash
source .env
airflow standalone
```
This prints an admin password and serves the UI at **http://localhost:8080**.
Log in as `admin`.

**5. Process a paper.**
Drop a PDF into your **papers/** folder, then in the UI enable the **paper_pipeline**
DAG and click **Trigger**. When it finishes, open `output/index.html` and check
your Discord channel.

To run the whole DAG once from the terminal (useful for debugging):
```bash
airflow dags test paper_pipeline
```

## Notes

- Database schema: `filename` (primary key), `title`, `summary`,
  `keywords` (JSON string), `category`, `created_at`.
