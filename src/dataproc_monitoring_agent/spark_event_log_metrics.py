#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Read a Spark event log from GCS and print key performance metrics as JSON,
including app-level, job-level, and stage-level summaries.

Usage:
  python read_spark_eventlog_gcs.py gs://<bucket>/events/spark-job-history/<application_id> [--project <PROJECT_ID>]

Requires:
  pip install google-cloud-storage
"""

import io, os, sys, json, gzip, math, argparse
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from google.cloud import storage

# ---------------- Utilities ----------------

def parse_size_to_bytes(s: Optional[str]) -> Optional[int]:
    if s is None: return None
    s = s.strip().lower()
    if not s: return None
    if s.isdigit(): return int(s)
    mult = 1
    if s.endswith("k"): mult, s = 1024, s[:-1]
    elif s.endswith("m"): mult, s = 1024**2, s[:-1]
    elif s.endswith("g"): mult, s = 1024**3, s[:-1]
    elif s.endswith("t"): mult, s = 1024**4, s[:-1]
    try: return int(float(s) * mult)
    except ValueError: return None

def bytes_to_gb(n: float) -> float:
    return float(n) / (1024**3)

def pct(sorted_list: List[int], p: float) -> int:
    if not sorted_list: return 0
    idx = max(0, min(len(sorted_list)-1, int(math.ceil(p * len(sorted_list))) - 1))
    return int(sorted_list[idx])

def detect_project_id(cli_project: Optional[str]) -> Optional[str]:
    if cli_project: return cli_project
    for k in ("GCP_PROJECT", "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"):
        v = os.getenv(k)
        if v: return v
    cred = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred and os.path.isfile(cred):
        try:
            with open(cred, "r", encoding="utf-8") as f:
                pj = json.load(f)
            pid = pj.get("project_id")
            if pid: return pid
        except Exception:
            pass
    return None

def open_gcs_text(client: storage.Client, gs_uri: str) -> io.TextIOBase:
    assert gs_uri.startswith("gs://"), f"Invalid GCS URI: {gs_uri}"
    _, path = gs_uri.split("gs://", 1)
    bucket_name, blob_name = path.split("/", 1)
    blob = client.bucket(bucket_name).blob(blob_name)
    data = blob.download_as_bytes()
    if blob_name.endswith(".gz") or (len(data) >= 2 and data[:2] == b"\x1f\x8b"):
        return io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(data)), encoding="utf-8")
    return io.TextIOWrapper(io.BytesIO(data), encoding="utf-8")

# ---------------- Core parsing ----------------

def parse_event_log(text_stream: io.TextIOBase) -> Dict[str, Any]:
    app = {"app_id": None, "app_name": None, "app_start_ts": None, "app_end_ts": None}
    spark_props: Dict[str, str] = {}
    executors: Dict[str, Dict[str, Any]] = {}
    live_executors = set()
    executor_peak = 0

    jobs: Dict[int, Dict[str, Any]] = {}
    stages_meta: Dict[Tuple[int, int], Dict[str, Any]] = {}
    stage_task_durations: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    stage_task_metrics: Dict[Tuple[int, int], Dict[str, int]] = defaultdict(lambda: {
        "tasks": 0, "failed_tasks": 0,
        "input_bytes": 0, "records_read": 0,
        "shuffle_read_bytes": 0, "shuffle_write_bytes": 0,
        "spill_disk_bytes": 0, "spill_mem_bytes": 0,
        "executorRunTime_ms": 0, "jvmGCTime_ms": 0,
        "resultSerializationTime_ms": 0, "executorDeserializeTime_ms": 0,
        "shuffleReadTime_ms": 0, "shuffleWriteTime_ms": 0
    })

    task_time_total_ms = 0

    for raw in text_stream:
        line = raw.strip()
        if not line: continue
        try:
            ev = json.loads(line)
        except Exception:
            continue
        et = ev.get("Event")

        # Application
        if et == "SparkListenerApplicationStart":
            app["app_id"] = ev.get("App ID")
            app["app_name"] = ev.get("App Name")
            app["app_start_ts"] = ev.get("Timestamp")
            for kv in ev.get("Spark Properties", []):
                if isinstance(kv, list) and len(kv) >= 2:
                    spark_props[str(kv[0])] = str(kv[1])
        elif et == "SparkListenerApplicationEnd":
            app["app_end_ts"] = ev.get("Timestamp")

        # Executors
        elif et == "SparkListenerExecutorAdded":
            ex_id = str(ev.get("Executor ID"))
            added_ts = ev.get("Timestamp") or ev.get("Time")
            total_cores = ev.get("Executor Info", {}).get("Total Cores") or ev.get("Total Cores") or spark_props.get("spark.executor.cores")
            try: total_cores = int(total_cores)
            except Exception: total_cores = None
            executors[ex_id] = {"added": added_ts, "removed": None, "cores": total_cores}
            live_executors.add(ex_id)
            executor_peak = max(executor_peak, len(live_executors))
        elif et == "SparkListenerExecutorRemoved":
            ex_id = str(ev.get("Executor ID"))
            removed_ts = ev.get("Timestamp") or ev.get("Time")
            if ex_id in executors: executors[ex_id]["removed"] = removed_ts
            live_executors.discard(ex_id)

        # Jobs
        elif et == "SparkListenerJobStart":
            jid = int(ev["Job ID"])
            jobs.setdefault(jid, {"job_id": jid, "name": None, "submission_ts": None, "completion_ts": None,
                                  "stage_ids": set(), "num_tasks": 0, "failed_tasks": 0,
                                  "input_bytes": 0, "shuffle_read_bytes": 0, "shuffle_write_bytes": 0,
                                  "task_time_ms": 0})
            jobs[jid]["submission_ts"] = ev.get("Submission Time")
            props = ev.get("Properties") or {}
            if props.get("spark.job.description"):
                jobs[jid]["name"] = props["spark.job.description"]
            for sid in ev.get("Stage IDs", []):
                jobs[jid]["stage_ids"].add(int(sid))
        elif et == "SparkListenerJobEnd":
            jid = int(ev["Job ID"])
            jobs.setdefault(jid, {"job_id": jid, "name": None, "submission_ts": None, "completion_ts": None,
                                  "stage_ids": set(), "num_tasks": 0, "failed_tasks": 0,
                                  "input_bytes": 0, "shuffle_read_bytes": 0, "shuffle_write_bytes": 0,
                                  "task_time_ms": 0})
            jobs[jid]["completion_ts"] = ev.get("Completion Time")

        # Stages
        elif et == "SparkListenerStageSubmitted":
            si = ev.get("Stage Info", {})
            sid = int(si.get("Stage ID"))
            att = int(si.get("Attempt ID", 0))
            stages_meta.setdefault((sid, att), {
                "stage_id": sid, "attempt": att, "name": si.get("Stage Name"),
                "submission_ts": ev.get("Submission Time"), "completion_ts": None,
                "num_tasks": si.get("Number of Tasks", 0)
            })
        elif et == "SparkListenerStageCompleted":
            si = ev.get("Stage Info", {})
            sid = int(si.get("Stage ID"))
            att = int(si.get("Attempt ID", 0))
            st = stages_meta.setdefault((sid, att), {
                "stage_id": sid, "attempt": att, "name": si.get("Stage Name"),
                "submission_ts": None, "completion_ts": None,
                "num_tasks": si.get("Number of Tasks", 0)
            })
            st["completion_ts"] = ev.get("Completion Time")

        # Tasks
        elif et == "SparkListenerTaskEnd":
            sid = int(ev.get("Stage ID"))
            satt = int(ev.get("Stage Attempt ID", 0))
            key = (sid, satt)

            task_info = ev.get("Task Info", {}) or {}
            failed = bool(task_info.get("Failed", False))
            finish = int(task_info.get("Finish Time", 0) or 0)
            launch = int(task_info.get("Launch Time", 0) or 0)
            dur = max(0, finish - launch)
            stage_task_durations[key].append(dur)

            tm = ev.get("Task Metrics") or {}
            I = tm.get("Input Metrics") or {}
            SR = tm.get("Shuffle Read Metrics") or {}
            SW = tm.get("Shuffle Write Metrics") or {}

            stm = stage_task_metrics[key]
            stm["tasks"] += 1
            if failed: stm["failed_tasks"] += 1

            stm["input_bytes"]        += int(I.get("Bytes Read", 0) or 0)
            stm["records_read"]       += int(I.get("Records Read", 0) or 0)
            stm["shuffle_read_bytes"] += int(SR.get("Remote Bytes Read", 0) or 0) + int(SR.get("Local Bytes Read", 0) or 0)
            stm["shuffle_write_bytes"]+= int(SW.get("Shuffle Bytes Written", 0) or 0)
            stm["spill_disk_bytes"]   += int(tm.get("Disk Bytes Spilled", 0) or 0)
            stm["spill_mem_bytes"]    += int(tm.get("Memory Bytes Spilled", 0) or 0)

            run_ms   = int(tm.get("Executor Run Time", 0) or 0)
            gc_ms    = int(tm.get("JVM GC Time", 0) or 0)
            ser_ms   = int(tm.get("Result Serialization Time", 0) or 0)
            deser_ms = int(tm.get("Executor Deserialize Time", 0) or 0)
            sr_ms    = int(SR.get("Fetch Wait Time", 0) or 0)
            sw_ms    = int(SW.get("Shuffle Write Time", 0) or 0)

            stm["executorRunTime_ms"]        += run_ms
            stm["jvmGCTime_ms"]              += gc_ms
            stm["resultSerializationTime_ms"]+= ser_ms
            stm["executorDeserializeTime_ms"]+= deser_ms
            stm["shuffleReadTime_ms"]        += sr_ms
            stm["shuffleWriteTime_ms"]       += sw_ms

            task_time_total_ms += run_ms

    # App totals from stages
    totals = {"input":0, "shuf_r":0, "shuf_w":0, "spill_d":0, "spill_m":0, "gc":0}
    for stm in stage_task_metrics.values():
        totals["input"] += stm["input_bytes"]
        totals["shuf_r"]+= stm["shuffle_read_bytes"]
        totals["shuf_w"]+= stm["shuffle_write_bytes"]
        totals["spill_d"]+= stm["spill_disk_bytes"]
        totals["spill_m"]+= stm["spill_mem_bytes"]
        totals["gc"]    += stm["jvmGCTime_ms"]

    # Per-job rollup
    for jid, j in jobs.items():
        stage_keys = [(sid, att) for (sid, att) in stages_meta.keys() if sid in j["stage_ids"]]
        j["num_tasks"] = sum(stage_task_metrics[k]["tasks"] for k in stage_keys)
        j["failed_tasks"] = sum(stage_task_metrics[k]["failed_tasks"] for k in stage_keys)
        j["input_bytes"] = sum(stage_task_metrics[k]["input_bytes"] for k in stage_keys)
        j["shuffle_read_bytes"] = sum(stage_task_metrics[k]["shuffle_read_bytes"] for k in stage_keys)
        j["shuffle_write_bytes"] = sum(stage_task_metrics[k]["shuffle_write_bytes"] for k in stage_keys)
        j["task_time_ms"] = sum(stage_task_metrics[k]["executorRunTime_ms"] for k in stage_keys)
        all_durs = sorted([d for k in stage_keys for d in stage_task_durations.get(k, [])])
        sub, comp = j.get("submission_ts"), j.get("completion_ts")
        j["job_duration_ms"] = int(comp - sub) if (sub and comp) else None
        j["p95_task_duration_ms"] = pct(all_durs, 0.95)
        j["p99_task_duration_ms"] = pct(all_durs, 0.99)
        j["max_task_duration_ms"] = int(all_durs[-1]) if all_durs else 0
        if all_durs:
            med = pct(all_durs, 0.50)
            j["max_over_median_ratio"] = (j["max_task_duration_ms"] / med) if med else None
        else:
            j["max_over_median_ratio"] = None

    # Stage-level finalization (quantiles, duration, scheduler delay approx)
    # Also compute reverse mapping: stage_id -> job_ids that include it
    stage_to_jobs: Dict[int, List[int]] = defaultdict(list)
    for jid, j in jobs.items():
        for sid in j["stage_ids"]:
            stage_to_jobs[sid].append(jid)

    stages_out: List[Dict[str, Any]] = []
    for (sid, att), meta in stages_meta.items():
        durs = sorted(stage_task_durations.get((sid, att), []))
        stm = stage_task_metrics[(sid, att)]
        sub = meta.get("submission_ts")
        comp = meta.get("completion_ts")

        stage_duration_ms = int(comp - sub) if (sub and comp) else None
        sum_task_wall_ms = int(sum(durs)) if durs else 0
        # Scheduler delay approx at stage level (floored at 0)
        accounted = (stm["executorRunTime_ms"] + stm["jvmGCTime_ms"] +
                     stm["resultSerializationTime_ms"] + stm["executorDeserializeTime_ms"] +
                     stm["shuffleReadTime_ms"] + stm["shuffleWriteTime_ms"])
        scheduler_delay_ms = max(0, sum_task_wall_ms - accounted)

        stages_out.append({
            "stage_id": sid,
            "attempt": att,
            "name": meta.get("name"),
            "job_ids": sorted(stage_to_jobs.get(sid, [])),
            "submission_ts": sub,
            "completion_ts": comp,
            "stage_duration_ms": stage_duration_ms,
            "num_tasks": int(meta.get("num_tasks") or 0),
            "failed_tasks": stm["failed_tasks"],
            "p50_task_duration_ms": pct(durs, 0.50),
            "p95_task_duration_ms": pct(durs, 0.95),
            "p99_task_duration_ms": pct(durs, 0.99),
            "sum_task_wall_time_ms": sum_task_wall_ms,
            "executorRunTime_ms": stm["executorRunTime_ms"],
            "jvmGCTime_ms": stm["jvmGCTime_ms"],
            "resultSerializationTime_ms": stm["resultSerializationTime_ms"],
            "executorDeserializeTime_ms": stm["executorDeserializeTime_ms"],
            "shuffleReadTime_ms": stm["shuffleReadTime_ms"],
            "shuffleWriteTime_ms": stm["shuffleWriteTime_ms"],
            "scheduler_delay_approx_ms": scheduler_delay_ms,
            "input_bytes": stm["input_bytes"],
            "records_read": stm["records_read"],
            "shuffle_read_bytes": stm["shuffle_read_bytes"],
            "shuffle_write_bytes": stm["shuffle_write_bytes"],
            "spill_disk_bytes": stm["spill_disk_bytes"],
            "spill_mem_bytes": stm["spill_mem_bytes"],
        })

    # App metrics
    app_duration_ms = (int(app["app_end_ts"] - app["app_start_ts"])
                       if app.get("app_start_ts") and app.get("app_end_ts") else None)
    app_metrics = {
        "app_id": app.get("app_id"),
        "app_name": app.get("app_name"),
        "app_start_ts": app.get("app_start_ts"),
        "app_end_ts": app.get("app_end_ts"),
        "app_duration_ms": app_duration_ms,
        "executor_peak": executor_peak,
        "input_bytes_total": int(totals["input"]),
        "shuffle_read_bytes_total": int(totals["shuf_r"]),
        "shuffle_write_bytes_total": int(totals["shuf_w"]),
        "spill_disk_bytes_total": int(totals["spill_d"]),
        "spill_mem_bytes_total": int(totals["spill_m"]),
        "gc_time_ms_total": int(totals["gc"]),
        "task_time_total_ms": int(task_time_total_ms),
    }

    return {
        "app": app_metrics,
        "jobs": jobs,
        "stages": stages_out,            # NEW: stage-level summaries
        "executors": executors,
        "spark_props": spark_props,
        "task_time_total_ms": task_time_total_ms,
    }

# ------------- Resource seconds -------------

def compute_resource_seconds(parsed: Dict[str, Any]) -> Tuple[float, float, Dict[int, Tuple[float, float]]]:
    app = parsed["app"]
    executors = parsed["executors"]
    spark_props = parsed["spark_props"]
    jobs = parsed["jobs"]
    task_time_total_ms = parsed["task_time_total_ms"]

    try:
        default_cores = int(spark_props.get("spark.executor.cores")) if spark_props.get("spark.executor.cores") else 1
    except Exception:
        default_cores = 1

    exec_mem_bytes = parse_size_to_bytes(spark_props.get("spark.executor.memory")) or parse_size_to_bytes("4g")
    overhead_bytes = parse_size_to_bytes(spark_props.get("spark.executor.memoryOverhead"))
    overhead_factor = None
    try:
        if spark_props.get("spark.executor.memoryOverheadFactor") is not None:
            overhead_factor = float(spark_props["spark.executor.memoryOverheadFactor"])
    except Exception:
        overhead_factor = None
    if overhead_bytes is None:
        overhead_bytes = int(exec_mem_bytes * overhead_factor) if overhead_factor is not None \
                         else max(384 * 1024 * 1024, int(0.10 * exec_mem_bytes))
    per_exec_mem_gb = bytes_to_gb(exec_mem_bytes + overhead_bytes)

    app_end = app.get("app_end_ts") or app.get("app_start_ts")
    vcore_seconds = 0.0
    mem_gb_seconds = 0.0
    for _, info in executors.items():
        added = info.get("added"); removed = info.get("removed") or app_end
        if not added or not removed: continue
        lifetime_sec = max(0.0, float(removed - added) / 1000.0)
        cores = info.get("cores") or default_cores
        vcore_seconds += float(cores) * lifetime_sec
        mem_gb_seconds += per_exec_mem_gb * lifetime_sec

    alloc: Dict[int, Tuple[float, float]] = {}
    denom = float(task_time_total_ms) if task_time_total_ms else 0.0
    for jid, j in jobs.items():
        w = (float(j.get("task_time_ms", 0)) / denom) if denom > 0 else 0.0
        alloc[jid] = (vcore_seconds * w, mem_gb_seconds * w)

    return vcore_seconds, mem_gb_seconds, alloc

# ---------------- Entrypoint ----------------

def main():
    ap = argparse.ArgumentParser(description="Read Spark event log from GCS and print metrics as JSON.")
    ap.add_argument("gcs_uri", help="gs://<bucket>/events/spark-job-history/<application_id>")
    ap.add_argument("--project", help="GCP project ID (or set GCP_PROJECT / GOOGLE_CLOUD_PROJECT)")
    args = ap.parse_args()

    project_id = detect_project_id(args.project)
    if not project_id:
        print(json.dumps({"error": "GCP project ID not found. Pass --project or set GCP_PROJECT/GOOGLE_CLOUD_PROJECT or ensure your SA JSON has project_id."}))
        sys.exit(2)

    try:
        gcs_client = storage.Client(project=project_id)
        with open_gcs_text(gcs_client, args.gcs_uri) as fh:
            parsed = parse_event_log(fh)
        app_vcores, app_mem_gbsec, job_alloc = compute_resource_seconds(parsed)

        out = {
            "app": {
                **parsed["app"],
                "app_vcore_seconds": app_vcores,
                "app_memory_gb_seconds": app_mem_gbsec,
                "spark_executor_memory": parsed["spark_props"].get("spark.executor.memory"),
                "spark_executor_cores": parsed["spark_props"].get("spark.executor.cores"),
                "spark_executor_memoryOverhead": parsed["spark_props"].get("spark.executor.memoryOverhead"),
            },
            "jobs": [],
            "stages": []   # NEW
        }

        # jobs
        for jid, j in sorted(parsed["jobs"].items(), key=lambda kv: kv[0]):
            vsec, msec = job_alloc.get(jid, (0.0, 0.0))
            out["jobs"].append({
                "job_id": int(jid),
                "job_name": j.get("name"),
                "job_duration_ms": j.get("job_duration_ms"),
                "num_tasks": j.get("num_tasks"),
                "failed_tasks": j.get("failed_tasks"),
                "input_bytes": j.get("input_bytes"),
                "shuffle_read_bytes": j.get("shuffle_read_bytes"),
                "shuffle_write_bytes": j.get("shuffle_write_bytes"),
                "task_time_ms": j.get("task_time_ms"),
                "p95_task_duration_ms": j.get("p95_task_duration_ms"),
                "p99_task_duration_ms": j.get("p99_task_duration_ms"),
                "max_task_duration_ms": j.get("max_task_duration_ms"),
                "max_over_median_ratio": j.get("max_over_median_ratio"),
                "job_vcore_seconds": vsec,
                "job_memory_gb_seconds": msec
            })

        # stages (sorted by stage_duration_ms desc; None at the end)
        def _stage_sort_key(s):
            return (-1 if s["stage_duration_ms"] is None else -s["stage_duration_ms"], s["stage_id"], s["attempt"])

        out["stages"] = sorted(parsed["stages"], key=_stage_sort_key)

        print(json.dumps(out, indent=2))

    except Exception as e:
        print(json.dumps({"error": f"Failed to read/parse event log: {str(e)}"}))
        sys.exit(1)

if __name__ == "__main__":
    main()
