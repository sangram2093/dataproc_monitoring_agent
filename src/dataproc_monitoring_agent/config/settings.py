"""Configuration helpers for the Dataproc monitoring agent."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional


@dataclass(slots=True)
class MonitoringConfig:
    """Runtime configuration for the Dataproc monitoring agent.

    Environment variables (all optional unless noted):
      * DATAPROC_PROJECT_ID (required): GCP project to query.
      * DATAPROC_REGION (required): Primary Dataproc region.
      * DATAPROC_LOOKBACK_HOURS: Lookback window for job ingestion.
      * DATAPROC_BASELINE_DAYS: Trailing window for baseline statistics.
      * DATAPROC_BQ_DATASET: BigQuery dataset used for the performance memory.
      * DATAPROC_BQ_TABLE: BigQuery table for daily facts.
      * DATAPROC_BQ_LOCATION: Optional BigQuery dataset location.
      * DATAPROC_EVENTLOG_BUCKET: GCS bucket where Spark event logs are stored.
      * DATAPROC_EVENTLOG_PREFIX: Optional prefix within the bucket.
      * DATAPROC_MAX_EVENTLOG_BYTES: Guard-rail for Spark event log downloads.
      * DATAPROC_DRY_RUN: When set to "true", skips writes to BigQuery.
    """

    project_id: str
    region: str
    lookback_hours: int = 24
    baseline_days: int = 7
    bq_dataset: str = "dataproc_monitoring"
    bq_table: str = "daily_facts"
    bq_location: Optional[str] = None
    eventlog_bucket: Optional[str] = None
    eventlog_prefix: str = ""
    max_eventlog_bytes: int = 50_000_000
    dry_run: bool = False

    @property
    def lookback(self) -> timedelta:
        """Timedelta representation of the ingestion lookback window."""
        return timedelta(hours=self.lookback_hours)

    @property
    def baseline_window(self) -> timedelta:
        """Timedelta representation for the baseline trailing window."""
        return timedelta(days=self.baseline_days)

    @property
    def fully_qualified_table(self) -> str:
        """BigQuery table identifier of the performance memory."""
        return f"{self.project_id}.{self.bq_dataset}.{self.bq_table}"

    @classmethod
    def from_env(cls) -> "MonitoringConfig":
        """Populate configuration values from process environment variables."""

        def _require(name: str) -> str:
            value = os.getenv(name)
            if not value:
                raise ValueError(f"Missing required environment variable: {name}")
            return value

        project_id = _require("DATAPROC_PROJECT_ID")
        region = _require("DATAPROC_REGION")

        lookback_hours = int(os.getenv("DATAPROC_LOOKBACK_HOURS", "24"))
        baseline_days = int(os.getenv("DATAPROC_BASELINE_DAYS", "7"))
        bq_dataset = os.getenv("DATAPROC_BQ_DATASET", "dataproc_monitoring")
        bq_table = os.getenv("DATAPROC_BQ_TABLE", "daily_facts")
        bq_location = os.getenv("DATAPROC_BQ_LOCATION") or None
        eventlog_bucket = os.getenv("DATAPROC_EVENTLOG_BUCKET") or None
        eventlog_prefix = os.getenv("DATAPROC_EVENTLOG_PREFIX", "")
        max_eventlog_bytes = int(os.getenv("DATAPROC_MAX_EVENTLOG_BYTES", "50000000"))
        dry_run = os.getenv("DATAPROC_DRY_RUN", "false").lower() in {"1", "true", "yes"}

        return cls(
            project_id=project_id,
            region=region,
            lookback_hours=lookback_hours,
            baseline_days=baseline_days,
            bq_dataset=bq_dataset,
            bq_table=bq_table,
            bq_location=bq_location,
            eventlog_bucket=eventlog_bucket,
            eventlog_prefix=eventlog_prefix,
            max_eventlog_bytes=max_eventlog_bytes,
            dry_run=dry_run,
        )

    @classmethod
    def from_overrides(cls, overrides: dict[str, object]) -> "MonitoringConfig":
        """Factory that accepts explicit overrides without relying on env vars."""

        try:
            project_id = str(overrides["project_id"])
            region = str(overrides["region"])
        except KeyError as exc:  # pragma: no cover - defensive
            raise ValueError("project_id and region must be supplied") from exc

        return cls(
            project_id=project_id,
            region=region,
            lookback_hours=int(overrides.get("lookback_hours", 24)),
            baseline_days=int(overrides.get("baseline_days", 7)),
            bq_dataset=str(overrides.get("bq_dataset", "dataproc_monitoring")),
            bq_table=str(overrides.get("bq_table", "daily_facts")),
            bq_location=overrides.get("bq_location") or None,
            eventlog_bucket=overrides.get("eventlog_bucket") or None,
            eventlog_prefix=str(overrides.get("eventlog_prefix", "")),
            max_eventlog_bytes=int(overrides.get("max_eventlog_bytes", 50_000_000)),
            dry_run=str(overrides.get("dry_run", "false")).lower()
            in {"1", "true", "yes"},
        )


def load_config(overrides: Optional[dict[str, object]] = None) -> MonitoringConfig:
    """Factory helper to stitch together configuration from env + overrides."""

    overrides = overrides or {}

    try:
        config = MonitoringConfig.from_env()
    except ValueError as env_error:
        if "project_id" in overrides and "region" in overrides:
            return MonitoringConfig.from_overrides(overrides)
        raise env_error

    if not overrides:
        return config

    merged = {**config.__dict__, **overrides}
    return MonitoringConfig.from_overrides(merged)
