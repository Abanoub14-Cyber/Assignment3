# Research Paper Pipeline — INFO-H420 Assignment 3

A local data pipeline, orchestrated with **Apache Airflow**, that turns a folder
of research PDFs into a searchable study library. For every new PDF it converts
the file to Markdown, asks a **locally served LLM** (via LM Studio or Ollama) for a title,
summary, keywords and category, stores that metadata in **SQLite**, regenerates a
static **HTML** page with search and category filtering, and sends a **Discord**
notification.

```
scan → convert → summarize → store → build_html → notify
```

## Project layout

```
paper-pipeline/
├── dags/
│   ├── paper_pipeline.py   # the Airflow DAG 
│   ├── paper_lib.py        # all the logic (DB, PDF, LLM, HTML, Discord)
│   └── template.html       # HTML template for the generated page
├── requirements.txt
├── install.sh              # installation script
├── .env.example            # copy to .env and fill in
├── .gitignore
└── README.md
```

PDFs and the generated index.html live in the Google Drive–synced folder.

## Prerequisites

- [LM Studio](https://lmstudio.ai) or [Ollama](https://ollama.com/) (serves the local LLM)
- A local model installed
- Python 3.12 (recommended version)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) utility installed (recommended) to create the virtual environment

## Setup and run
**1. Configure**
```bash
cp .env.example .env
```
Edit `.env`: set the Google Drive paths and paste your **Discord webhook URL**
(Discord → Edit Channel → Integrations → Webhooks → New Webhook → Copy URL).

---

**2. Create a virtual environment and install the dependencies.**
```bash
uv venv
source .venv/bin/activate
source .env
./install.sh
```

The install script handles downloading the required packages as well as apache airflow, pinned to its proper contraints file based on the airflow version and the downloaded python version.

If you do not want to use the uv package, replace `uv venv` with `python3.12 -m venv .venv` and remove all mention of uv inside **install.sh**

---

**3. Start the local LLM.**
In LM Studio: download **Gemma 3 1B Instruct**, open the *Developer* tab, load the
model, and **start the server** (it serves `http://localhost:1234/v1`). Similar steps can be done for Ollama, which serves its server at `http://localhost:11434/v1`

---

**4. Run Airflow.**
```bash
airflow standalone
```
This prints an admin password and serves the UI at **http://localhost:8080**.
Log in as `admin`.

---

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

### Google Drive setup
- On windows, [download](https://ipv4.google.com/intl/en_zm/drive/download/) the google drive desktop app
- On WSL, mount the google drive disk (usually under the name G:)
```bash
sudo mkdir -p /mnt/g
sudo mount -t drvfs G: /mnt/g
```
- Create two folders in the drive: One that acts as the inbox for the papers, and the other will hold the output html file to access the parsed library. Suggested folder structure:
```
Research Library/
├── papers/
│   ├── paper1.pdf   
│   ├── paper2.pdf
│   └── ...
└── output/
    └── index.html
```