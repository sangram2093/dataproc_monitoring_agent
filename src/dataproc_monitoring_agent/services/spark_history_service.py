"""Helpers to access Spark event logs stored in GCS."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from google.api_core import exceptions
from google.cloud import storage

from ..config.settings import MonitoringConfig


@dataclass(slots=True)
class SparkEventLog:
    blob_name: str
    size_bytes: int
    content_snippet: str


_EVENTLOG_NAME_HINTS = (
    "application_",  # Default Spark naming scheme
    "job_",
)


def fetch_spark_event_logs(
    config: MonitoringConfig,
    *,
    application_ids: Iterable[str],
) -> list[SparkEventLog]:
    """Download Spark event logs from GCS for the supplied application ids."""

    if not config.eventlog_bucket:
        return []

    client = storage.Client(project=config.project_id)
    bucket = client.bucket(config.eventlog_bucket)

    logs: list[SparkEventLog] = []
    byte_cap = max(config.max_eventlog_bytes, 1024)

    for app_id in application_ids:
        prefix_parts = [
            config.eventlog_prefix.rstrip("/") if config.eventlog_prefix else "",
            app_id,
        ]
        prefix = "/".join(part for part in prefix_parts if part)
        for blob in client.list_blobs(bucket, prefix=prefix):
            if not any(hint in blob.name for hint in _EVENTLOG_NAME_HINTS):
                continue
            try:
                raw_bytes = blob.download_as_bytes(start=0, end=byte_cap - 1)
            except exceptions.NotFound:
                continue
            except exceptions.GoogleAPICallError as exc:
                raise RuntimeError(
                    f"Failed downloading Spark event log {blob.name}: {exc}"
                ) from exc
            logs.append(
                SparkEventLog(
                    blob_name=blob.name,
                    size_bytes=blob.size or len(raw_bytes),
                    content_snippet=raw_bytes.decode("utf-8", errors="replace"),
                )
            )
    return logs
