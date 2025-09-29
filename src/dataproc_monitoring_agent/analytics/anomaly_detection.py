"""Simple heuristic-based anomaly detectors for Dataproc runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..analytics.performance_memory import BaselineStats
from ..repositories.bigquery_repository import DataprocFact
from ..services.monitoring_service import MetricSeries


@dataclass(slots=True)
class Anomaly:
    kind: str
    severity: str
    message: str
    evidence: dict


def detect_job_runtime_anomaly(
    fact: DataprocFact,
    baseline: BaselineStats | None,
) -> Anomaly | None:
    """Spot jobs exceeding their historical runtime baselines."""

    if not fact.duration_seconds or fact.duration_seconds <= 0:
        return None
    if not baseline or not baseline.p50_duration:
        return None

    ratio = fact.duration_seconds / baseline.p50_duration
    if ratio < 1.5:
        return None

    severity = "warning"
    if ratio >= 2.0:
        severity = "critical"

    return Anomaly(
        kind="job_runtime_regression",
        severity=severity,
        message=(
            f"Job {fact.job_id} runtime {fact.duration_seconds:.1f}s is "
            f"{ratio:.1f}x slower than median ({baseline.p50_duration:.1f}s)."
        ),
        evidence={
            "job_id": fact.job_id,
            "duration_seconds": fact.duration_seconds,
            "baseline_p50_seconds": baseline.p50_duration,
            "baseline_p95_seconds": baseline.p95_duration,
            "baseline_run_count": baseline.run_count,
        },
    )


def detect_cluster_underutilization(
    fact: DataprocFact,
    cluster_metrics: Iterable[MetricSeries],
) -> Anomaly | None:
    """Detect idle clusters based on worker idle cores metric."""

    idle_metric = next(
        (
            series
            for series in cluster_metrics
            if series.metric_type.endswith("/idle_cores")
        ),
        None,
    )
    if not idle_metric or not idle_metric.points:
        return None

    avg_idle = sum(point.value for point in idle_metric.points) / len(idle_metric.points)
    if avg_idle < 1:
        return None

    return Anomaly(
        kind="cluster_underutilized",
        severity="info",
        message=(
            f"Cluster {fact.cluster_name} averaged {avg_idle:.1f} idle cores while running {fact.job_id}."
        ),
        evidence={
            "cluster_name": fact.cluster_name,
            "avg_idle_cores": avg_idle,
            "metric_series": idle_metric.metric_type,
        },
    )


def synthesize_anomaly_flags(
    fact: DataprocFact,
    *,
    baseline: BaselineStats | None,
    cluster_metrics: Iterable[MetricSeries],
) -> dict:
    """Aggregate anomalies into a JSON-ready representation."""

    findings: list[Anomaly] = []

    runtime_anomaly = detect_job_runtime_anomaly(fact, baseline)
    if runtime_anomaly:
        findings.append(runtime_anomaly)

    under_util = detect_cluster_underutilization(fact, cluster_metrics)
    if under_util:
        findings.append(under_util)

    return {
        "findings": [
            {
                "kind": finding.kind,
                "severity": finding.severity,
                "message": finding.message,
                "evidence": finding.evidence,
            }
            for finding in findings
        ],
        "has_issues": any(finding.severity in {"warning", "critical"} for finding in findings),
    }
