"""Cloud Monitoring helpers for Dataproc signal ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from google.api_core import exceptions
try:
    from google.cloud import monitoring_v3
    from google.protobuf.timestamp_pb2 import Timestamp
except ImportError as exc:  # pragma: no cover - dependency optional at import time
    monitoring_v3 = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

from ..config.settings import MonitoringConfig


@dataclass(slots=True)
class MetricPoint:
    timestamp: str
    value: float


@dataclass(slots=True)
class MetricSeries:
    metric_type: str
    resource_type: str
    resource_labels: dict[str, str]
    metric_labels: dict[str, str]
    points: list[MetricPoint]


_CLUSTER_METRICS = (
    "dataproc.googleapis.com/cluster/yarn/memory_utilization",
    "dataproc.googleapis.com/cluster/hdfs/percent_used",
    "dataproc.googleapis.com/cluster/cpu/utilization",
    "dataproc.googleapis.com/agent/worker/idle_cores",
)

_JOB_METRICS = (
    "dataproc.googleapis.com/job/runtime",
    "dataproc.googleapis.com/job/stage_elapsed_time",
)


def fetch_cluster_metrics(
    config: MonitoringConfig,
    *,
    cluster_name: str,
    start_time: datetime,
    end_time: datetime,
    metric_types: Iterable[str] | None = None,
) -> list[MetricSeries]:
    """Pull Dataproc cluster metrics from Cloud Monitoring."""

    metric_types = tuple(metric_types or _CLUSTER_METRICS)
    filter_parts = [
        f"metric.type = \"{metric_type}\"" for metric_type in metric_types
    ]
    filter_expr = " OR ".join(filter_parts)
    filter_expr = (
        f"({filter_expr}) AND resource.type = \"cloud_dataproc_cluster\" "
        f"AND resource.label.\"cluster_name\" = \"{cluster_name}\" "
        f"AND resource.label.\"region\" = \"{config.region}\""
    )
    return _list_time_series(
        config,
        filter_expr=filter_expr,
        start_time=start_time,
        end_time=end_time,
    )




def fetch_job_metrics(
    config: MonitoringConfig,
    *,
    job_id: str,
    start_time: datetime,
    end_time: datetime,
    metric_types: Iterable[str] | None = None,
) -> list[MetricSeries]:
    """Pull Dataproc job metrics from Cloud Monitoring."""

    metric_types = tuple(metric_types or _JOB_METRICS)

    results: list[MetricSeries] = []
    for metric_type in metric_types:
        filter_expr = (
            f'metric.type = "{metric_type}" '
            f'AND resource.type = "cloud_dataproc_job" '
            f'AND metric.label."job_id" = "{job_id}"'
        )
        results.extend(
            _list_time_series(
                config,
                filter_expr=filter_expr,
                start_time=start_time,
                end_time=end_time,
            )
        )

    return results


def _list_time_series(
    config: MonitoringConfig,
    *,
    filter_expr: str,
    start_time: datetime,
    end_time: datetime,
) -> list[MetricSeries]:
    _ensure_client_available()
    client = monitoring_v3.MetricServiceClient()
    interval = monitoring_v3.TimeInterval(
        end_time=_to_timestamp(end_time),
        start_time=_to_timestamp(start_time),
    )

    request = monitoring_v3.ListTimeSeriesRequest(
        name=f"projects/{config.project_id}",
        filter=filter_expr,
        interval=interval,
        view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    )

    series: list[MetricSeries] = []
    try:
        for ts in client.list_time_series(request=request):
            points = [
                MetricPoint(
                    timestamp=point.interval.end_time.isoformat(),
                    value=_extract_value(point.value),
                )
                for point in ts.points
            ]
            series.append(
                MetricSeries(
                    metric_type=ts.metric.type,
                    resource_type=ts.resource.type,
                    resource_labels=dict(ts.resource.labels),
                    metric_labels=dict(ts.metric.labels),
                    points=points,
                )
            )
    except exceptions.GoogleAPICallError as exc:
        raise RuntimeError(
            f"Failed to query Monitoring API for filter: {filter_expr}: {exc}"
        ) from exc
    return series


def _extract_value(value: monitoring_v3.types.TypedValue) -> float:
    if value.double_value is not None:
        return float(value.double_value)
    if value.int64_value is not None:
        return float(value.int64_value)
    if value.distribution_value.count:
        return float(value.distribution_value.count)
    return 0.0


def _to_timestamp(moment: datetime) -> monitoring_v3.Timestamp:
    if not moment.tzinfo:
        moment = moment.replace(tzinfo=timezone.utc)
    ts = Timestamp()
    ts.FromDatetime(moment.astimezone(timezone.utc))
    return ts


def _ensure_client_available() -> None:
    if _IMPORT_ERROR is not None:
        raise ImportError(
            "google-cloud-monitoring is required for monitoring queries"
        ) from _IMPORT_ERROR
