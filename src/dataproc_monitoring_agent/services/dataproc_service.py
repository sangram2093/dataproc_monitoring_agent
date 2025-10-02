"""Dataproc API helpers for cluster and job discovery."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, List, Optional

from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import Cluster
from google.cloud.dataproc_v1.types import Job

from ..config.settings import MonitoringConfig



@dataclass(slots=True)
class ClusterSnapshot:
    """Serializable view of a Dataproc cluster at ingestion time."""

    project_id: str
    region: str
    cluster_name: str
    uuid: str
    status: str
    software_config: dict[str, Any]
    metrics: dict[str, Any]
    labels: dict[str, str]
    cluster_uuid: Optional[str]
    create_time: Optional[str]
    update_time: Optional[str]

    @classmethod
    def from_api(cls, project_id: str, region: str, cluster: Cluster) -> "ClusterSnapshot":
        metrics: dict[str, Any] = {}
        if getattr(cluster, 'metrics', None):
            metrics = {
                "hdfs_metrics": dict(cluster.metrics.hdfs_metrics),
                "yarn_metrics": dict(cluster.metrics.yarn_metrics),
            }

        status = getattr(cluster, 'status', None)
        status_state = getattr(status, 'state', None)
        status_state_name = status_state.name if status_state else "UNKNOWN"
        status_start_time = getattr(status, 'state_start_time', None)

        history = getattr(status, 'history', None)
        history_entry = None
        if history:
            try:
                history_entry = history[0]
            except (TypeError, IndexError):
                history_entry = None

        create_time = _to_rfc3339(getattr(history_entry, 'state_start_time', None))
        update_time = _to_rfc3339(status_start_time)

        software_config: dict[str, Any] = {}
        if getattr(cluster, 'config', None) and getattr(cluster.config, 'software_config', None):
            soft_cfg = cluster.config.software_config
            software_config = {
                "image_version": soft_cfg.image_version,
                "optional_components": list(soft_cfg.optional_components),
                "properties": dict(soft_cfg.properties),
            }

        return cls(
            project_id=project_id,
            region=region,
            cluster_name=cluster.cluster_name,
            uuid=cluster.cluster_uuid,
            status=status_state_name,
            software_config=software_config,
            metrics=metrics,
            labels=dict(cluster.labels),
            cluster_uuid=cluster.cluster_uuid or None,
            create_time=create_time,
            update_time=update_time,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a BigQuery friendly payload."""
        return asdict(self)




@dataclass(slots=True)
class JobSnapshot:
    """High level Dataproc job execution metadata."""

    project_id: str
    region: str
    job_id: str
    job_type: str | None
    cluster_name: Optional[str]
    cluster_uuid: Optional[str]
    reference: dict[str, Any]
    state: str
    substate: Optional[str]
    driver_output_uri: Optional[str]
    driver_control_files_uri: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    state_timestamp: Optional[str]
    yarn_application_ids: list[str] = field(default_factory=list)
    status_history: list[dict[str, Any]] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_api(cls, project_id: str, region: str, job: Job) -> "JobSnapshot":
        yarn_ids: list[str] = []
        if getattr(job, "yarn_applications", None):
            for app in job.yarn_applications:
                yarn_id = getattr(app, "application_id", None) or getattr(app, "name", None)
                if yarn_id:
                    yarn_ids.append(yarn_id)

        status_history: list[dict[str, Any]] = []
        if getattr(job, "status_history", None):
            for status in job.status_history:
                status_history.append(
                    {
                        "state": getattr(getattr(status, "state", None), "name", None),
                        "state_start_time": _to_rfc3339(getattr(status, "state_start_time", None)),
                        "details": getattr(status, "details", None),
                    }
                )

        placement = job.placement or dataproc_v1.types.JobPlacement()
        reference = job.reference or dataproc_v1.types.JobReference()

        job_uuid = getattr(job, "job_uuid", "")
        job_id = reference.job_id if getattr(reference, "job_id", None) else job_uuid

        status = getattr(job, "status", None)
        job_state = getattr(getattr(status, "state", None), "name", None) if status else "UNKNOWN"
        job_substate = (
            getattr(getattr(status, "substate", None), "name", None)
            if status and getattr(status, "substate", None)
            else None
        )

        start_time = _to_rfc3339(getattr(status, "start_time", None)) if status else None
        end_time = _to_rfc3339(getattr(status, "end_time", None)) if status else None
        state_ts = _to_rfc3339(getattr(status, "state_start_time", None)) if status else None

        return cls(
            project_id=project_id,
            region=region,
            job_id=job_id,
            job_type=getattr(getattr(job, "type_", None), "name", None),
            cluster_name=getattr(placement, "cluster_name", None) or None,
            cluster_uuid=getattr(placement, "cluster_uuid", None) or None,
            reference={
                "job_id": reference.job_id,
                "project_id": reference.project_id,
                "region": region,
            }
            if getattr(reference, "job_id", None)
            else {},
            state=job_state,
            substate=job_substate,
            driver_output_uri=getattr(job, "driver_output_resource_uri", None),
            driver_control_files_uri=getattr(job, "driver_control_files_uri", None),
            start_time=start_time,
            end_time=end_time,
            state_timestamp=state_ts,
            yarn_application_ids=yarn_ids,
            status_history=status_history,
            labels=dict(job.labels),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)



def _to_rfc3339(timestamp: Optional[datetime]) -> Optional[str]:
    if not timestamp:
        return None
    if not timestamp.tzinfo:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).isoformat()


def list_clusters(config: MonitoringConfig) -> List[ClusterSnapshot]:
    """Fetch the current set of Dataproc clusters for the configured region."""
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{config.region}-dataproc.googleapis.com:443"}
    )
    request = dataproc_v1.ListClustersRequest(
        project_id=config.project_id, region=config.region
    )
    clusters: List[ClusterSnapshot] = []
    for cluster in client.list_clusters(request=request):
        clusters.append(
            ClusterSnapshot.from_api(config.project_id, config.region, cluster)
        )
    return clusters


def list_jobs_within_window(
    config: MonitoringConfig,
    *,
    start_time: datetime,
    end_time: datetime,
) -> List[JobSnapshot]:
    """Retrieve Dataproc jobs submitted within the specified window."""

    client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{config.region}-dataproc.googleapis.com:443"}
    )
    request = dataproc_v1.ListJobsRequest(
        project_id=config.project_id,
        region=config.region,
    )

    window_start = start_time.astimezone(timezone.utc)
    window_end = end_time.astimezone(timezone.utc)

    jobs: List[JobSnapshot] = []
    for job in client.list_jobs(request=request):
        submit_timestamp = _job_submission_time(job)
        if submit_timestamp:
            submit_timestamp = submit_timestamp.astimezone(timezone.utc)
            if submit_timestamp < window_start:
                continue
            if submit_timestamp > window_end:
                continue
        jobs.append(JobSnapshot.from_api(config.project_id, config.region, job))

    return jobs



def _job_submission_time(job: Job) -> Optional[datetime]:
    """Best-effort extraction of job submission timestamp."""

    if job.status and job.status.state_start_time:
        return job.status.state_start_time
    if job.status_history:
        return job.status_history[0].state_start_time
    return None
