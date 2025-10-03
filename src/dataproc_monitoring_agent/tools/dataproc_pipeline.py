"""Tool functions orchestrating the Dataproc monitoring pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
import json

from google.adk.tools.tool_context import ToolContext

from ..analytics.anomaly_detection import synthesize_anomaly_flags
from ..analytics.performance_memory import load_baselines
from ..config.settings import MonitoringConfig, load_config
from ..reporting.report_builder import build_status_report
from ..repositories.bigquery_repository import (
    DataprocFact,
    ensure_performance_table,
    insert_daily_facts,
    utc_now,
)
from ..services import dataproc_service
from ..services import logging_service
from ..services import monitoring_service
from ..services import spark_history_service


def ingest_dataproc_signals(
    *,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
    lookback_hours: Optional[int] = None,
    tool_context: Optional[ToolContext] = None,
) -> dict[str, Any]:
    """Collect Dataproc signals (clusters, jobs, metrics, logs, event logs)."""

    config = _resolve_config(
        project_id=project_id,
        region=region,
        lookback_hours=lookback_hours,
    )
    end_time = utc_now()
    start_time = end_time - config.lookback

    clusters = dataproc_service.list_clusters(config)
    jobs = dataproc_service.list_jobs_within_window(
        config, start_time=start_time, end_time=end_time
    )

    cluster_metrics: dict[str, List[monitoring_service.MetricSeries]] = {}
    for cluster in clusters:
        series = monitoring_service.fetch_cluster_metrics(
            config,
            cluster_name=cluster.cluster_name,
            start_time=start_time,
            end_time=end_time,
        )
        cluster_metrics[cluster.cluster_name] = series

    job_payloads: list[dict[str, Any]] = []
    for job in jobs:
        job_metrics = monitoring_service.fetch_job_metrics(
            config,
            job_id=job.job_id,
            start_time=start_time,
            end_time=end_time,
        )
        driver_logs = []
        if job.cluster_name:
            driver_logs = logging_service.fetch_driver_logs(
                config,
                cluster_name=job.cluster_name,
                start_time=start_time,
                end_time=end_time,
                limit=400,
            )
        yarn_logs: list[logging_service.LogLine] = []
        for yarn_id in job.yarn_application_ids:
            yarn_logs.extend(
                logging_service.fetch_yarn_container_logs(
                    config,
                    yarn_application_id=yarn_id,
                    start_time=start_time,
                    end_time=end_time,
                    limit=200,
                )
            )
        spark_logs = spark_history_service.fetch_spark_event_logs(
            config, application_ids=job.yarn_application_ids
        )
        job_payloads.append(
            {
                "job": job,
                "job_metrics": job_metrics,
                "driver_logs": driver_logs,
                "yarn_logs": yarn_logs,
                "spark_event_logs": spark_logs,
            }
        )

    ingestion_payload = {
        "window": {
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
        },
        "clusters": clusters,
        "cluster_metrics": cluster_metrics,
        "jobs": job_payloads,
    }

    if tool_context is not None:
        tool_context.state["dataproc_ingestion"] = ingestion_payload

    return {
        "window": ingestion_payload["window"],
        "cluster_count": len(clusters),
        "job_count": len(job_payloads),
    }


def build_performance_memory(
    *,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
    tool_context: Optional[ToolContext] = None,
) -> dict[str, Any]:
    """Persist Dataproc job observations into BigQuery with anomaly flags."""

    config = _resolve_config(project_id=project_id, region=region)
    ingestion_payload = None
    if tool_context is not None:
        ingestion_payload = tool_context.state.get("dataproc_ingestion")
    if ingestion_payload is None:
        message = (
            "No Dataproc signals are cached yet. Run ingest_dataproc_signals before "
            "building performance memory."
        )
        return {
            "persisted_rows": 0,
            "dry_run": config.dry_run,
            "has_anomalies": False,
            "message": message,
        }

    ensure_performance_table(config)

    now = utc_now()
    baselines = load_baselines(
        config,
        as_of=now,
        trailing_window=config.baseline_window,
    )

    facts: list[DataprocFact] = []
    for job_bundle in ingestion_payload["jobs"]:
        job = job_bundle["job"]
        cluster_metrics = ingestion_payload["cluster_metrics"].get(
            job.cluster_name or "", []
        )
        fact = _build_fact(
            config=config,
            as_of=now,
            job=job,
            job_metrics=job_bundle["job_metrics"],
            driver_logs=job_bundle["driver_logs"],
            yarn_logs=job_bundle["yarn_logs"],
            spark_event_logs=job_bundle["spark_event_logs"],
            cluster_metrics=cluster_metrics,
            baselines=baselines,
        )
        facts.append(fact)

    insert_daily_facts(config, records=facts)

    serialized = [fact.to_json() for fact in facts]
    if tool_context is not None:
        tool_context.state["dataproc_facts"] = serialized

    return {
        "persisted_rows": len(serialized),
        "dry_run": config.dry_run,
        "has_anomalies": any(
            fact.anomaly_flags.get("has_issues") for fact in facts
        ),
    }


def generate_dataproc_report(
    *,
    tool_context: Optional[ToolContext] = None,
) -> dict[str, Any]:
    """Return a human readable Dataproc status report."""

    facts_payload = None
    if tool_context is not None:
        facts_payload = tool_context.state.get("dataproc_facts")

    if not facts_payload:
        report = (
            "No Dataproc facts are cached yet. Run build_performance_memory before requesting a report."
        )
        if tool_context is not None:
            tool_context.state["dataproc_report"] = report
        return {"report": report}

    report = build_status_report(
        [
            DataprocFact(**fact)
            for fact in facts_payload
        ]
    )

    if tool_context is not None:
        tool_context.state["dataproc_report"] = report

    return {"report": report}


def _resolve_config(**overrides: Any) -> MonitoringConfig:
    usable_overrides = {
        key: value
        for key, value in overrides.items()
        if value is not None
    }
    if usable_overrides:
        return load_config(usable_overrides)
    return load_config()


def _build_fact(
    *,
    config: MonitoringConfig,
    as_of: datetime,
    job: dataproc_service.JobSnapshot,
    job_metrics: List[monitoring_service.MetricSeries],
    driver_logs: List[logging_service.LogLine],
    yarn_logs: List[logging_service.LogLine],
    spark_event_logs: List[spark_history_service.SparkEventLog],
    cluster_metrics: List[monitoring_service.MetricSeries],
    baselines: Dict[str, Any],
) -> DataprocFact:
    job_id = job.job_id or job.reference.get("job_id") or ""
    duration_seconds = _compute_duration(job.start_time, job.end_time)

    fact = DataprocFact(
        ingest_date=as_of.date().isoformat(),
        ingest_timestamp=as_of.isoformat(),
        project_id=config.project_id,
        region=config.region,
        cluster_name=job.cluster_name or "unknown",
        job_id=job_id,
        job_type=job.job_type,
        job_state=job.state,
        job_start_time=job.start_time,
        job_end_time=job.end_time,
        duration_seconds=duration_seconds,
        yarn_application_ids=list(job.yarn_application_ids),
        cluster_metrics=_metric_series_to_json(cluster_metrics),
        job_metrics=_metric_series_to_json(job_metrics),
        driver_log_excerpt=_summarize_logs(driver_logs),
        yarn_log_excerpt=_summarize_logs(yarn_logs),
        spark_event_snippet=_summarize_event_logs(spark_event_logs),
        anomaly_flags={},
    )

    baseline = baselines.get(job_id)
    fact.anomaly_flags = synthesize_anomaly_flags(
        fact,
        baseline=baseline,
        cluster_metrics=cluster_metrics,
    )
    fact.anomaly_flags = json.dumps(fact.anomaly_flags) if fact.anomaly_flags else None
    return fact


def _metric_series_to_json(
    series_list: Iterable[monitoring_service.MetricSeries],
) -> Optional[str]:
    payload: list[dict[str, Any]] = []
    for series in series_list:
        payload.append(
            {
                "metric_type": series.metric_type,
                "resource_type": series.resource_type,
                "resource_labels": series.resource_labels,
                "metric_labels": series.metric_labels,
                "points": [
                    {"timestamp": point.timestamp, "value": point.value}
                    for point in series.points
                ],
            }
        )
    if not payload:
        return None
    return json.dumps(payload)

def _summarize_logs(logs: Iterable[logging_service.LogLine], *, limit: int = 10) -> str | None:
    snippets: list[str] = []
    for idx, entry in enumerate(logs):
        if idx >= limit:
            break
        snippets.append(
            f"[{entry.timestamp}] {entry.severity}: {entry.text.strip()}"
        )
    if not snippets:
        return None
    combined = "\n".join(snippets)
    return combined[:4000]


def _summarize_event_logs(
    logs: Iterable[spark_history_service.SparkEventLog], *, limit: int = 2
) -> str | None:
    snippets: list[str] = []
    for idx, event_log in enumerate(logs):
        if idx >= limit:
            break
        snippets.append(
            f"{event_log.blob_name} (size={event_log.size_bytes}):\n{event_log.content_snippet[:2000]}"
        )
    if not snippets:
        return None
    combined = "\n\n".join(snippets)
    return combined[:6000]


def _compute_duration(start_iso: Optional[str], end_iso: Optional[str]) -> float | None:
    if not start_iso or not end_iso:
        return None
    try:
        start = datetime.fromisoformat(start_iso)
        end = datetime.fromisoformat(end_iso)
    except ValueError:
        return None
    if not start.tzinfo:
        start = start.replace(tzinfo=timezone.utc)
    if not end.tzinfo:
        end = end.replace(tzinfo=timezone.utc)
    return max((end - start).total_seconds(), 0.0)

