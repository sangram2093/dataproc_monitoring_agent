"""Baselines and historical memory backed by BigQuery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable

from google.api_core import exceptions
from google.cloud import bigquery

from ..config.settings import MonitoringConfig


@dataclass(slots=True)
class BaselineStats:
    job_id: str
    job_type: str
    cluster_name: str
    p50_duration: float | None
    p95_duration: float | None
    avg_duration: float | None
    run_count: int


def load_baselines(
    config: MonitoringConfig,
    *,
    as_of: datetime,
    trailing_window: timedelta,
) -> dict[str, BaselineStats]:
    """Load trailing baselines per job from BigQuery."""

    client = bigquery.Client(project=config.project_id)
    query = f"""
        WITH history AS (
          SELECT
            job_id,
            job_type,
            cluster_name,
            duration_seconds
          FROM `{config.fully_qualified_table}`
          WHERE ingest_timestamp BETWEEN @window_start AND @as_of
            AND duration_seconds IS NOT NULL
        )
        SELECT
          job_id,
          ANY_VALUE(job_type) AS job_type,
          ANY_VALUE(cluster_name) AS cluster_name,
          APPROX_QUANTILES(duration_seconds, 20)[OFFSET(10)] AS p50_duration,
          APPROX_QUANTILES(duration_seconds, 20)[OFFSET(18)] AS p95_duration,
          AVG(duration_seconds) AS avg_duration,
          COUNT(*) AS run_count
        FROM history
        GROUP BY job_id
    """

    params = [
        bigquery.ScalarQueryParameter(
            "window_start",
            "TIMESTAMP",
            (as_of - trailing_window).isoformat(),
        ),
        bigquery.ScalarQueryParameter("as_of", "TIMESTAMP", as_of.isoformat()),
    ]

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        result = client.query(query, job_config=job_config).result()
    except exceptions.GoogleAPICallError as exc:
        raise RuntimeError(f"Failed loading baselines: {exc}") from exc

    baselines: dict[str, BaselineStats] = {}
    for row in result:
        baselines[row.job_id] = BaselineStats(
            job_id=row.job_id,
            job_type=row.job_type,
            cluster_name=row.cluster_name,
            p50_duration=row.p50_duration,
            p95_duration=row.p95_duration,
            avg_duration=row.avg_duration,
            run_count=row.run_count,
        )
    return baselines


def fetch_recent_jobs(
    config: MonitoringConfig,
    *,
    limit: int = 50,
) -> Iterable[dict]:
    """Return the most recent runs persisted in the performance table."""

    client = bigquery.Client(project=config.project_id)
    query = f"""
        SELECT *
        FROM `{config.fully_qualified_table}`
        ORDER BY ingest_timestamp DESC
        LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    try:
        result = client.query(query, job_config=job_config).result()
    except exceptions.GoogleAPICallError as exc:
        raise RuntimeError(f"Failed fetching recent jobs: {exc}") from exc

    for row in result:
        yield dict(row.items())
