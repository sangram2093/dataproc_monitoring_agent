"""Entrypoint helpers to execute the Dataproc monitoring agent."""

from __future__ import annotations

import argparse
import asyncio
import time
from typing import Optional

from google.adk import Runner
from google.adk.artifacts.in_memory_artifact_service import InMemoryArtifactService
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai import types

from .agents.dataproc_agent import build_dataproc_monitoring_agent


_DEFAULT_PROMPT = (
    "Execute the Dataproc monitoring guard-railed playbook over the last 24 "
    "hours and return the synthesized status report."
)


def run_once(prompt: Optional[str] = None, *, model: Optional[str] = None) -> str:
    """Run a single monitoring cycle and return the final report string."""

    agent = build_dataproc_monitoring_agent(model=model)
    session_service = InMemorySessionService()
    artifact_service = InMemoryArtifactService()

    runner = Runner(
        app_name="dataproc-monitor",
        agent=agent,
        session_service=session_service,
        artifact_service=artifact_service,
    )

    session_id = f"run-{int(time.time())}"

    asyncio.run(
        session_service.create_session(
            app_name="dataproc-monitor",
            user_id="operator",
            session_id=session_id,
        )
    )

    request = types.Content(
        role="user",
        parts=[types.Part(text=prompt or _DEFAULT_PROMPT)],
    )

    final_response: str = ""
    for event in runner.run(
        user_id="operator",
        session_id=session_id,
        new_message=request,
    ):
        if event.author == agent.name and event.is_final_response():
            final_response = _content_to_text(event.content)

    return final_response


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Dataproc monitoring agent once and print the report.",
    )
    parser.add_argument(
        "--prompt",
        help="Optional custom instruction delivered to the orchestrator agent.",
    )
    parser.add_argument(
        "--model",
        help="Override the foundational model used by the agents.",
    )
    args = parser.parse_args()

    report = run_once(prompt=args.prompt, model=args.model)
    print(report)


def _content_to_text(content: types.Content | None) -> str:
    if not content or not content.parts:
        return ""
    chunks: list[str] = []
    for part in content.parts:
        if part.text:
            chunks.append(part.text)
        elif part.function_response and part.function_response.response:
            chunks.append(str(part.function_response.response))
    return "".join(chunks)


if __name__ == "__main__":
    main()
