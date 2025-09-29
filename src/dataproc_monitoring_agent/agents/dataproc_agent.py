"""ADK agent hierarchy for the Dataproc monitoring specialist."""

from __future__ import annotations

import os

from google.adk import Agent
from google.adk.tools.function_tool import FunctionTool

from ..tools import dataproc_pipeline


DEFAULT_MODEL = os.getenv("DATAPROC_AGENT_MODEL", "models/gemini-1.5-pro")


def build_dataproc_monitoring_agent(model: str | None = None) -> Agent:
    """Instantiate the Dataproc monitoring agent tree."""

    collector = Agent(
        name="dataproc_collector",
        description="Collects Dataproc clusters, jobs, metrics, logs and Spark event logs.",
        instruction=(
            "Use the `ingest_dataproc_signals` tool to gather telemetry for the configured "
            "lookback window. Confirm the counts returned and share noteworthy raw findings."),
        tools=[FunctionTool(dataproc_pipeline.ingest_dataproc_signals)],
    )

    memory_builder = Agent(
        name="performance_memory_builder",
        description="Persists Dataproc job observations into BigQuery and tags anomalies.",
        instruction=(
            "After signals are collected, call `build_performance_memory` to compute daily facts, "
            "compare against baselines, and persist to BigQuery. Mention whether rows were written "
            "or if dry-run mode was active."),
        tools=[FunctionTool(dataproc_pipeline.build_performance_memory)],
    )

    reporter = Agent(
        name="dataproc_reporter",
        description="Generates a concise status report for operators.",
        instruction=(
            "Once the performance memory is refreshed, invoke `generate_dataproc_report` and return "
            "the formatted report to the orchestrator."),
        tools=[FunctionTool(dataproc_pipeline.generate_dataproc_report)],
    )

    orchestrator_instruction = (
        "You are the Dataproc monitoring orchestrator. Execute the guard-railed runbook: "
        "1) delegate to dataproc_collector, 2) delegate to performance_memory_builder, "
        "3) delegate to dataproc_reporter. Ensure prerequisites are satisfied before each step, "
        "echo critical anomalies uncovered, and conclude with the generated status report."
    )

    orchestrator = Agent(
        name="dataproc_orchestrator",
        description="Coordinates the Dataproc monitoring pipeline end-to-end.",
        instruction=orchestrator_instruction,
        model=model or DEFAULT_MODEL,
        sub_agents=[collector, memory_builder, reporter],
    )
    return orchestrator
