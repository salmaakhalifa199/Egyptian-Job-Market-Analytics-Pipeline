# Egyptian Job Market Analytics Pipeline

An end-to-end data engineering project that tracks the Egyptian tech job market
using Wuzzuf scraping, Kafka streaming, Airflow orchestration, PostgreSQL
warehousing, and a Looker Studio / IBM Cognos dashboard.

---

## Project Structure

```
egyptian_job_pipeline/
├── scraper/
│   ├── wuzzuf_scraper.py     # Wuzzuf HTML scraper
│   └── validator.py          # Data quality checker
├── kafka/
│   ├── producer.py           # Kafka producer (Phase 2)
│   └── consumer.py           # Kafka consumer (Phase 2)
├── airflow/
│   └── dags/
│       └── job_pipeline_dag.py  # Airflow DAG (Phase 3)
├── warehouse/
│   ├── schema.sql            # Star schema DDL (Phase 4)
│   └── load.py               # DW loader (Phase 4)
├── dashboard/                # Looker Studio config (Phase 5)
├── data/
│   ├── raw/                  # Raw JSON from scraper
│   └── staging/              # Cleaned data before DW load
├── logs/                     # Scraper + pipeline logs
├── config/
│   └── .env.example          # Environment variable template
├── run_scraper.sh            # Shell entry point
└── requirements.txt
```

---

## Setup

### 1. Clone and install dependencies
```bash
git clone <your-repo>
cd egyptian_job_pipeline
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp config/.env.example config/.env
# Edit config/.env with your PostgreSQL credentials
```

---

## Phase 1 — Scraping

### Run the scraper
```bash
# Basic usage
bash run_scraper.sh "data engineer" 5

# Other keywords to try
bash run_scraper.sh "data analyst" 5
bash run_scraper.sh "machine learning" 3
bash run_scraper.sh "backend developer" 5
```

### Validate scraped data
```bash
# Validate a single file
python scraper/validator.py --file data/raw/wuzzuf_data_engineer_20260412_120000.json

# Validate all files in the raw directory
python scraper/validator.py --dir data/raw
```

Output files are saved to `data/raw/` as:
```
wuzzuf_{keyword}_{timestamp}.json
```

### JSON schema (one job object)
```json
{
  "job_id":      "abc123",
  "title":       "Data Engineer",
  "company":     "Vodafone Egypt",
  "location":    "Cairo, Egypt",
  "job_type":    "Full Time",
  "experience":  "2 - 4 Yrs of Exp",
  "skills":      ["Python", "SQL", "Apache Spark"],
  "posted_date": "3 days ago",
  "url":         "https://wuzzuf.net/jobs/p/...",
  "scraped_at":  "2026-04-12T10:00:00+00:00",
  "keyword":     "data engineer"
}
```

---

## Phases (Coming Next)

| Phase | What you'll build |
|-------|-------------------|
| 2     | ✔ Kafka producer + consumer — pipeline_kafka/ |
| 3     | Airflow DAG to orchestrate the full pipeline |
| 4     | PostgreSQL star schema data warehouse |
| 5     | Looker Studio / IBM Cognos BI dashboard |

---

## Tech Stack

| Layer       | Tool                            |
|-------------|----------------------------------|
| Ingestion   | Python · Shell · BeautifulSoup   |
| Streaming   | Apache Kafka                     |
| Orchestration | Apache Airflow                 |
| Storage     | PostgreSQL (staging + DW)        |
| BI          | Google Looker Studio / IBM Cognos|
