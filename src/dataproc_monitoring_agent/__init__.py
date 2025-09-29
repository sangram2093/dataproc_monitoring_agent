from __future__ import annotations

from typing import Optional

__all__ = [
    'build_dataproc_monitoring_agent',
    'run_once',
]


def build_dataproc_monitoring_agent(model: Optional[str] = None):
    from .agents.dataproc_agent import build_dataproc_monitoring_agent as _builder

    return _builder(model=model)


def run_once(prompt: Optional[str] = None, *, model: Optional[str] = None) -> str:
    from .runner import run_once as _run_once

    return _run_once(prompt=prompt, model=model)

