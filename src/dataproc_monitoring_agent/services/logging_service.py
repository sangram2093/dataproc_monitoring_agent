"""Cloud Logging access helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from google.api_core import exceptions
from google.cloud import logging_v2

from ..config.settings import MonitoringConfig


@dataclass(slots=True)
class LogLine:
    timestamp: str
    log_name: str
    severity: str
    text: str
    resource_labels: dict[str, str]
    labels: dict[str, str]


_DRIVER_FILTER_TEMPLATE = (
    'resource.type="cloud_dataproc_cluster" '
    'AND jsonPayload.logger="Driver" '
    'AND resource.labels."cluster_name"="{cluster_name}"'
)

_YARN_FILTER_TEMPLATE = (
    'resource.type="cloud_dataproc_worker" '
    'AND jsonPayload.logger="ContainerExecutor" '
    'AND jsonPayload.yarnAppId="{yarn_application_id}"'
)


def fetch_driver_logs(
    config: MonitoringConfig,
    *,
    cluster_name: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 2000,
) -> list[LogLine]:
    """Pull Dataproc driver logs for the supplied cluster."""

    filter_expr = _format_filter(
        _DRIVER_FILTER_TEMPLATE,
        start_time=start_time,
        end_time=end_time,
        cluster_name=cluster_name,
    )
    return list(_iterate_logs(config, filter_expr=filter_expr, limit=limit))


def fetch_yarn_container_logs(
    config: MonitoringConfig,
    *,
    yarn_application_id: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 2000,
) -> list[LogLine]:
    """Pull YARN container logs correlated to a Dataproc job."""

    filter_expr = _format_filter(
        _YARN_FILTER_TEMPLATE,
        start_time=start_time,
        end_time=end_time,
        yarn_application_id=yarn_application_id,
    )
    return list(_iterate_logs(config, filter_expr=filter_expr, limit=limit))


def _iterate_logs(
    config: MonitoringConfig,
    *,
    filter_expr: str,
    limit: int,
) -> Iterator[LogLine]:
    client = logging_v2.Client(project=config.project_id)
    try:
        for entry in client.list_entries(filter_=filter_expr, page_size=limit):
            payload_dict: dict[str, str] = {}
            if hasattr(entry, "payload") and isinstance(entry.payload, dict):
                payload_dict = entry.payload
            text = entry.text_payload or payload_dict.get("message", "")
            yield LogLine(
                timestamp=entry.timestamp.isoformat() if entry.timestamp else "",
                log_name=entry.log_name,
                severity=entry.severity,
                text=text,
                resource_labels=dict(entry.resource.labels or {}),
                labels=dict(entry.labels or {}),
            )
    except exceptions.GoogleAPICallError as exc:
        raise RuntimeError(f"Failed to fetch logs from Logging API: {exc}") from exc


def _format_filter(
    template: str,
    *,
    start_time: datetime,
    end_time: datetime,
    **kwargs: str,
) -> str:
    if not start_time.tzinfo:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if not end_time.tzinfo:
        end_time = end_time.replace(tzinfo=timezone.utc)

    time_clause = (
        f'timestamp>="{start_time.astimezone(timezone.utc).isoformat()}" '
        f'AND timestamp<="{end_time.astimezone(timezone.utc).isoformat()}"'
    )
    return f"{template.format(**kwargs)} AND {time_clause}"
