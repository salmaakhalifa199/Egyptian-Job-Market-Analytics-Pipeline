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
