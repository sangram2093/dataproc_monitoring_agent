"""Status report builder for the Dataproc monitoring agent."""

from __future__ import annotations

from collections import Counter
from typing import Iterable

from ..repositories.bigquery_repository import DataprocFact


def build_status_report(facts: Iterable[DataprocFact]) -> str:
    facts = list(facts)
    if not facts:
        return "No Dataproc activity detected within the configured window."

    status_counts = Counter(fact.job_state for fact in facts)
    anomalies = [
        fact for fact in facts if fact.anomaly_flags.get("has_issues")
    ]

    lines: list[str] = []
    lines.append("Dataproc monitoring summary")
    lines.append("==========================")
    lines.append(
        "Jobs ingested: {count} (states: {states})".format(
            count=len(facts),
            states=", ".join(f"{state}={count}" for state, count in status_counts.items()),
        )
    )

    if anomalies:
        lines.append("")
        lines.append(f"⚠️  {len(anomalies)} regression(s) detected:")
        for fact in anomalies:
            findings = fact.anomaly_flags.get("findings", [])
            if not findings:
                continue
            top = findings[0]
            lines.append(
                f"- {fact.job_id} on cluster {fact.cluster_name}: {top['message']}"
            )
    else:
        lines.append("")
        lines.append("No runtime regressions detected against current baselines.")

    lines.append("")
    lines.append("Recent job highlights:")
    for fact in facts[:5]:
        duration = "n/a"
        if fact.duration_seconds:
            duration = f"{fact.duration_seconds:.1f}s"
        lines.append(
            f"- {fact.job_id} ({fact.job_type}) state={fact.job_state} duration={duration}"
        )

    return "\n".join(lines)
