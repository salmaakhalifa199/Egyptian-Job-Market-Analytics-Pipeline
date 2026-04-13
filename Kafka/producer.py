"""
kafka/producer.py
─────────────────
Reads raw JSON files from data/raw/ and publishes each job
as a message to the Kafka topic defined in config/.env.

Phase 2 of the Egyptian Job Market Analytics Pipeline.

Usage:
    python kafka/producer.py --input-dir data/raw
    python kafka/producer.py --file data/raw/wuzzuf_data_engineer_mock.json
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

load_dotenv(dotenv_path=Path("config/.env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | PRODUCER | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config from .env ──────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "wuzzuf_raw_jobs")
SEND_DELAY        = float(os.getenv("PRODUCER_DELAY_MS", "50")) / 1000  # ms → s


# ── Producer factory ──────────────────────────────────────────────────────────
def make_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Reliability settings
        acks="all",           # wait for all in-sync replicas
        retries=3,
        retry_backoff_ms=300,
        # Throughput settings
        linger_ms=10,         # batch messages for 10ms before sending
        batch_size=16_384,    # 16 KB batch size
        compression_type="gzip",
    )


# ── Core send logic ───────────────────────────────────────────────────────────
def on_send_success(record_metadata):
    logger.debug(
        "Sent → topic=%s partition=%s offset=%s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc):
    logger.error("Failed to send message: %s", exc)


def publish_jobs(producer: KafkaProducer, jobs: list[dict], source_file: str) -> int:
    """
    Publish a list of job dicts to the raw Kafka topic.
    Returns number of messages successfully sent.
    """
    sent = 0
    for job in jobs:
        # Enrich with pipeline metadata
        job["_source_file"] = source_file
        job["_pipeline_stage"] = "raw"

        producer.send(
            topic=TOPIC_RAW,
            key=job.get("job_id"),          # partition by job_id for ordering
            value=job,
        ).add_callback(on_send_success).add_errback(on_send_error)

        sent += 1
        if SEND_DELAY > 0:
            time.sleep(SEND_DELAY)

    producer.flush()   # block until all pending messages are delivered
    return sent


# ── File loader ───────────────────────────────────────────────────────────────
def load_jobs_from_file(path: Path) -> list[dict]:
    """Load jobs list from a raw JSON file produced by the scraper."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    jobs = data.get("jobs", [])
    if not jobs:
        logger.warning("No jobs found in %s", path.name)
    return jobs


# ── Main ──────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka producer for Wuzzuf job data")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file",      type=Path, help="Single JSON file to publish")
    group.add_argument("--input-dir", type=Path, help="Directory of JSON files to publish")
    parser.add_argument(
        "--topic", default=TOPIC_RAW,
        help=f"Kafka topic to publish to (default: {TOPIC_RAW})"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Collect files to process
    if args.file:
        files = [args.file]
    else:
        files = sorted(args.input_dir.glob("*.json"))
        if not files:
            logger.error("No JSON files found in %s", args.input_dir)
            return

    logger.info("Connecting to Kafka at %s ...", BOOTSTRAP_SERVERS)
    try:
        producer = make_producer()
    except NoBrokersAvailable:
        logger.error(
            "Cannot connect to Kafka broker at %s. "
            "Make sure Kafka is running: docker-compose up -d",
            BOOTSTRAP_SERVERS,
        )
        return

    total_sent = 0
    for file_path in files:
        logger.info("Loading %s ...", file_path.name)
        jobs = load_jobs_from_file(file_path)
        if not jobs:
            continue
        sent = publish_jobs(producer, jobs, source_file=file_path.name)
        total_sent += sent
        logger.info("Published %d jobs from %s → topic '%s'", sent, file_path.name, TOPIC_RAW)

    producer.close()
    logger.info("Done. Total messages published: %d", total_sent)


if __name__ == "__main__":
    main()
