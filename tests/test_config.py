from dataproc_monitoring_agent.config.settings import load_config


def test_load_config_with_overrides(monkeypatch):
    monkeypatch.delenv("DATAPROC_PROJECT_ID", raising=False)
    monkeypatch.delenv("DATAPROC_REGION", raising=False)

    config = load_config(
        {
            "project_id": "demo-project",
            "region": "us-central1",
            "lookback_hours": 6,
            "dry_run": True,
        }
    )

    assert config.project_id == "demo-project"
    assert config.region == "us-central1"
    assert config.lookback_hours == 6
    assert config.dry_run is True
