"""BigQuery persistence layer for the Dataproc monitoring agent."""

from __future__ import annotations

from dataclasses import dataclass, asdict
import json
from datetime import datetime, timezone
from typing import Iterable

from google.api_core import exceptions
from google.api_core.exceptions import NotFound, Forbidden
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

    def __post_init__(self) -> None:
        if isinstance(self.cluster_metrics, str) and self.cluster_metrics:
            try:
                self.cluster_metrics = json.loads(self.cluster_metrics)
            except json.JSONDecodeError:
                self.cluster_metrics = {}
        elif self.cluster_metrics is None:
            self.cluster_metrics = {}

        if isinstance(self.job_metrics, str) and self.job_metrics:
            try:
                self.job_metrics = json.loads(self.job_metrics)
            except json.JSONDecodeError:
                self.job_metrics = {}
        elif self.job_metrics is None:
            self.job_metrics = {}

        if isinstance(self.anomaly_flags, str) and self.anomaly_flags:
            try:
                self.anomaly_flags = json.loads(self.anomaly_flags)
            except json.JSONDecodeError:
                self.anomaly_flags = {}
        elif self.anomaly_flags is None:
            self.anomaly_flags = {}

    def to_json(self) -> dict:
        payload = asdict(self)
        payload["cluster_metrics"] = json.dumps(payload["cluster_metrics"]) if payload["cluster_metrics"] else None
        payload["job_metrics"] = json.dumps(payload["job_metrics"]) if payload["job_metrics"] else None
        payload["anomaly_flags"] = json.dumps(payload["anomaly_flags"]) if payload["anomaly_flags"] else None
        return payload


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
    """Verify the performance dataset/table exist; raise if they do not."""

    client = bigquery.Client(project=config.project_id)

    dataset_ref = bigquery.DatasetReference(config.project_id, config.bq_dataset)
    try:
        client.get_dataset(dataset_ref)
    except NotFound as exc:
        raise RuntimeError(
            f"BigQuery dataset '{config.bq_dataset}' not found in project {config.project_id}. "
            "Create it manually before running the monitoring pipeline."
        ) from exc
    except Forbidden as exc:
        raise RuntimeError(
            "Insufficient permissions to read BigQuery dataset metadata. "
            "Grant bigquery.datasets.get on the dataset or run with DATAPROC_DRY_RUN=true."
        ) from exc

    table_ref = dataset_ref.table(config.bq_table)
    try:
        client.get_table(table_ref)
    except NotFound as exc:
        raise RuntimeError(
            f"BigQuery table '{config.bq_dataset}.{config.bq_table}' is missing. "
            "Create it with the documented schema before running the monitoring pipeline."
        ) from exc
    except Forbidden as exc:
        raise RuntimeError(
            "Insufficient permissions to access the BigQuery table. "
            "Grant bigquery.tables.get on the table or run with DATAPROC_DRY_RUN=true."
        ) from exc


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
