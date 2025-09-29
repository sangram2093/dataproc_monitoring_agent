"""BigQuery persistence layer for the Dataproc monitoring agent."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Iterable

from google.api_core import exceptions
from google.cloud import bigquery

from ..config.settings import MonitoringConfig


@dataclass(slots=True)
class DataprocFact:
    ingest_date: str
    ingest_timestamp: str
    project_id: str
    region: str
    cluster_name: str
    job_id: str
    job_type: str
    job_state: str
    job_start_time: str | None
    job_end_time: str | None
    duration_seconds: float | None
    yarn_application_ids: list[str]
    cluster_metrics: dict
    job_metrics: dict
    driver_log_excerpt: str | None
    yarn_log_excerpt: str | None
    spark_event_snippet: str | None
    anomaly_flags: dict

    def to_json(self) -> dict:
        return asdict(self)


_TABLE_SCHEMA = [
    bigquery.SchemaField("ingest_date", "DATE"),
    bigquery.SchemaField("ingest_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("project_id", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("cluster_name", "STRING"),
    bigquery.SchemaField("job_id", "STRING"),
    bigquery.SchemaField("job_type", "STRING"),
    bigquery.SchemaField("job_state", "STRING"),
    bigquery.SchemaField("job_start_time", "TIMESTAMP"),
    bigquery.SchemaField("job_end_time", "TIMESTAMP"),
    bigquery.SchemaField("duration_seconds", "FLOAT"),
    bigquery.SchemaField("yarn_application_ids", "STRING", mode="REPEATED"),
    bigquery.SchemaField("cluster_metrics", "JSON"),
    bigquery.SchemaField("job_metrics", "JSON"),
    bigquery.SchemaField("driver_log_excerpt", "STRING"),
    bigquery.SchemaField("yarn_log_excerpt", "STRING"),
    bigquery.SchemaField("spark_event_snippet", "STRING"),
    bigquery.SchemaField("anomaly_flags", "JSON"),
]


def ensure_performance_table(config: MonitoringConfig) -> None:
    """Create the backing dataset & table when they do not yet exist."""

    client = bigquery.Client(project=config.project_id)

    dataset_ref = bigquery.DatasetReference(config.project_id, config.bq_dataset)
    try:
        client.get_dataset(dataset_ref)
    except exceptions.NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        if config.bq_location:
            dataset.location = config.bq_location
        client.create_dataset(dataset)

    table_ref = dataset_ref.table(config.bq_table)
    try:
        client.get_table(table_ref)
    except exceptions.NotFound:
        table = bigquery.Table(table_ref, schema=_TABLE_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingest_date",
        )
        client.create_table(table)


def insert_daily_facts(
    config: MonitoringConfig,
    *,
    records: Iterable[DataprocFact],
) -> None:
    """Stream daily fact rows into BigQuery."""

    payload = [record.to_json() for record in records]
    if not payload:
        return

    if config.dry_run:
        return

    client = bigquery.Client(project=config.project_id)
    table_id = config.fully_qualified_table
    errors = client.insert_rows_json(table_id, payload)
    if errors:
        raise RuntimeError(f"Failed to insert rows into {table_id}: {errors}")


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
