#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

# --------- Helpers to flatten SparkPlanInfo & wire metrics ----------
def _walk_plan(plan_info: Dict[str, Any], exec_id: int, nodes: List[Dict[str, Any]], acc_map: Dict[int, Tuple[int,int,str]], parent_idx: Optional[int]=None) -> int:
    """Flatten SparkPlanInfo tree into nodes list; build accumulatorId -> (exec_id,node_idx,metric_name)."""
    node = {
        "node_index": len(nodes),
        "parent_index": parent_idx,
        "nodeName": plan_info.get("nodeName"),
        "simpleString": plan_info.get("simpleString"),
        "children": [],
        "metrics": {},     # name -> value (filled later from DriverAccumUpdates)
        "metricIds": {}    # name -> accumulatorId
    }
    # Bind metric IDs so we can fill values later
    for m in plan_info.get("metrics", []):
        name = m.get("name")
        acc_id = m.get("accumulatorId")
        if isinstance(acc_id, int) and name:
            node["metricIds"][name] = acc_id
            acc_map[acc_id] = (exec_id, node["node_index"], name)
            node["metrics"][name] = 0
    nodes.append(node)
    me = node["node_index"]
    for child in plan_info.get("children", []):
        ci = _walk_plan(child, exec_id, nodes, acc_map, me)
        node["children"].append(ci)
    return me

# ---------------- Core parsing ----------------
def parse_event_log(text_stream: io.TextIOBase) -> Dict[str, Any]:
    app = {"app_id": None, "app_name": None, "app_start_ts": None, "app_end_ts": None}
    spark_props: Dict[str, str] = {}
    executors: Dict[str, Dict[str, Any]] = {}
    live_executors = set(); executor_peak = 0

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

    # ---- SQL execution tracking ----
    sql_execs: Dict[int, Dict[str, Any]] = {}   # exec_id -> {...}
    acc_id_to_ref: Dict[int, Tuple[int,int,str]] = {}  # accId -> (exec_id, node_idx, metric_name)

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
            live_executors.add(ex_id); executor_peak = max(executor_peak, len(live_executors))
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
            if props.get("spark.job.description"): jobs[jid]["name"] = props["spark.job.description"]
            for sid in ev.get("Stage IDs", []): jobs[jid]["stage_ids"].add(int(sid))
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
            sid = int(si.get("Stage ID")); att = int(si.get("Attempt ID", 0))
            stages_meta.setdefault((sid, att), {
                "stage_id": sid, "attempt": att, "name": si.get("Stage Name"),
                "submission_ts": ev.get("Submission Time"), "completion_ts": None,
                "num_tasks": si.get("Number of Tasks", 0)
            })
        elif et == "SparkListenerStageCompleted":
            si = ev.get("Stage Info", {})
            sid = int(si.get("Stage ID")); att = int(si.get("Attempt ID", 0))
            st = stages_meta.setdefault((sid, att), {
                "stage_id": sid, "attempt": att, "name": si.get("Stage Name"),
                "submission_ts": None, "completion_ts": None,
                "num_tasks": si.get("Number of Tasks", 0)
            })
            st["completion_ts"] = ev.get("Completion Time")

        # Tasks
        elif et == "SparkListenerTaskEnd":
            sid = int(ev.get("Stage ID")); satt = int(ev.get("Stage Attempt ID", 0))
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
            stm["executorRunTime_ms"]        += int(tm.get("Executor Run Time", 0) or 0)
            stm["jvmGCTime_ms"]              += int(tm.get("JVM GC Time", 0) or 0)
            stm["resultSerializationTime_ms"]+= int(tm.get("Result Serialization Time", 0) or 0)
            stm["executorDeserializeTime_ms"]+= int(tm.get("Executor Deserialize Time", 0) or 0)
            stm["shuffleReadTime_ms"]        += int(SR.get("Fetch Wait Time", 0) or 0)
            stm["shuffleWriteTime_ms"]       += int(SW.get("Shuffle Write Time", 0) or 0)
            task_time_total_ms               += int(tm.get("Executor Run Time", 0) or 0)

        # ---------------- SQL events ----------------
        elif et in ("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart", "SparkListenerSQLExecutionStart"):
            exec_id = int(ev.get("Execution ID") or ev.get("executionId"))
            desc = ev.get("Description") or ev.get("description")
            details = ev.get("Details") or ev.get("details")  # may include logical/optimized plan text if explainMode is extended/formatted
            plan = ev.get("SparkPlanInfo") or ev.get("sparkPlanInfo") or {}
            sql_execs[exec_id] = {
                "execution_id": exec_id,
                "description": desc,
                "details": details,
                "start_ts": ev.get("Time") or ev.get("Timestamp"),
                "end_ts": None,
                "duration_ms": None,
                "initial_plan_nodes": [],
                "final_plan_nodes": None  # will be filled if AQE update arrives
            }
            _walk_plan(plan, exec_id, sql_execs[exec_id]["initial_plan_nodes"], acc_id_to_ref, None)

        elif et in ("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd", "SparkListenerSQLExecutionEnd"):
            exec_id = int(ev.get("Execution ID") or ev.get("executionId"))
            end_ts = ev.get("Time") or ev.get("Timestamp")
            if exec_id in sql_execs:
                sql_execs[exec_id]["end_ts"] = end_ts
                st = sql_execs[exec_id].get("start_ts")
                if st is not None and end_ts is not None:
                    sql_execs[exec_id]["duration_ms"] = int(end_ts - st)

        elif et in ("org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate", "SparkListenerSQLAdaptiveExecutionUpdate"):
            exec_id = int(ev.get("Execution ID") or ev.get("executionId"))
            plan = ev.get("SparkPlanInfo") or ev.get("sparkPlanInfo") or {}
            # Replace/update final plan nodes (fresh mapping for metric IDs; may differ)
            if exec_id in sql_execs:
                nodes: List[Dict[str, Any]] = []
                _walk_plan(plan, exec_id, nodes, acc_id_to_ref, None)
                sql_execs[exec_id]["final_plan_nodes"] = nodes

        elif et in ("org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates", "SparkListenerDriverAccumUpdates"):
            # Map accumulator values into the nodes
            # Structure: "accumUpdates": [[accId, newValue], ...]
            updates = ev.get("accumUpdates") or ev.get("Accumulable Updates") or []
            for upd in updates:
                try:
                    acc_id, val = int(upd[0]), int(upd[1])
                except Exception:
                    continue
                ref = acc_id_to_ref.get(acc_id)
                if not ref: 
                    continue
                exec_id, node_idx, mname = ref
                # Prefer final plan nodes (AQE) if present; else initial nodes
                target = None
                if exec_id in sql_execs and sql_execs[exec_id].get("final_plan_nodes"):
                    nodes = sql_execs[exec_id]["final_plan_nodes"]
                    if 0 <= node_idx < len(nodes): target = nodes[node_idx]
                elif exec_id in sql_execs:
                    nodes = sql_execs[exec_id]["initial_plan_nodes"]
                    if 0 <= node_idx < len(nodes): target = nodes[node_idx]
                if target is not None:
                    target.setdefault("metrics", {})
                    target["metrics"][mname] = val

    # ---- Roll-ups for app/jobs/stages ----
    totals = {"input":0, "shuf_r":0, "shuf_w":0, "spill_d":0, "spill_m":0, "gc":0}
    for stm in stage_task_metrics.values():
        totals["input"] += stm["input_bytes"]
        totals["shuf_r"]+= stm["shuffle_read_bytes"]
        totals["shuf_w"]+= stm["shuffle_write_bytes"]
        totals["spill_d"]+= stm["spill_disk_bytes"]
        totals["spill_m"]+= stm["spill_mem_bytes"]
        totals["gc"]    += stm["jvmGCTime_ms"]

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

    # Stage-level output
    stages_out: List[Dict[str, Any]] = []
    for (sid, att), meta in stages_meta.items():
        durs = sorted(stage_task_durations.get((sid, att), []))
        stm = stage_task_metrics[(sid, att)]
        sub = meta.get("submission_ts"); comp = meta.get("completion_ts")
        stage_duration_ms = int(comp - sub) if (sub and comp) else None
        sum_task_wall_ms = int(sum(durs)) if durs else 0
        accounted = (stm["executorRunTime_ms"] + stm["jvmGCTime_ms"] + stm["resultSerializationTime_ms"] +
                     stm["executorDeserializeTime_ms"] + stm["shuffleReadTime_ms"] + stm["shuffleWriteTime_ms"])
        scheduler_delay_ms = max(0, sum_task_wall_ms - accounted)
        stages_out.append({
            "stage_id": sid, "attempt": att, "name": meta.get("name"),
            "submission_ts": sub, "completion_ts": comp, "stage_duration_ms": stage_duration_ms,
            "num_tasks": int(meta.get("num_tasks") or 0), "failed_tasks": stm["failed_tasks"],
            "p50_task_duration_ms": pct(durs, 0.50), "p95_task_duration_ms": pct(durs, 0.95), "p99_task_duration_ms": pct(durs, 0.99),
            "sum_task_wall_time_ms": sum_task_wall_ms, "scheduler_delay_approx_ms": scheduler_delay_ms,
            "executorRunTime_ms": stm["executorRunTime_ms"], "jvmGCTime_ms": stm["jvmGCTime_ms"],
            "resultSerializationTime_ms": stm["resultSerializationTime_ms"], "executorDeserializeTime_ms": stm["executorDeserializeTime_ms"],
            "shuffleReadTime_ms": stm["shuffleReadTime_ms"], "shuffleWriteTime_ms": stm["shuffleWriteTime_ms"],
            "input_bytes": stm["input_bytes"], "records_read": stm["records_read"],
            "shuffle_read_bytes": stm["shuffle_read_bytes"], "shuffle_write_bytes": stm["shuffle_write_bytes"],
            "spill_disk_bytes": stm["spill_disk_bytes"], "spill_mem_bytes": stm["spill_mem_bytes"],
        })

    app_duration_ms = (int(app["app_end_ts"] - app["app_start_ts"])
                       if app.get("app_start_ts") and app.get("app_end_ts") else None)
    app_metrics = {
        "app_id": app.get("app_id"), "app_name": app.get("app_name"),
        "app_start_ts": app.get("app_start_ts"), "app_end_ts": app.get("app_end_ts"),
        "app_duration_ms": app_duration_ms, "executor_peak": executor_peak,
        "input_bytes_total": int(totals["input"]), "shuffle_read_bytes_total": int(totals["shuf_r"]),
        "shuffle_write_bytes_total": int(totals["shuf_w"]), "spill_disk_bytes_total": int(totals["spill_d"]),
        "spill_mem_bytes_total": int(totals["spill_m"]), "gc_time_ms_total": int(totals["gc"]),
        "task_time_total_ms": int(task_time_total_ms),
    }

    return {
        "app": app_metrics,
        "jobs": jobs,
        "stages": stages_out,
        "sql_execs": sql_execs,           # keep internal map for later shaping
        "executors": executors,
        "spark_props": spark_props,
        "task_time_total_ms": task_time_total_ms,
    }

# ------------- Resource seconds -------------
def compute_resource_seconds(parsed: Dict[str, Any]) -> Tuple[float, float, Dict[int, Tuple[float, float]]]:
    app = parsed["app"]; executors = parsed["executors"]; spark_props = parsed["spark_props"]; jobs = parsed["jobs"]
    task_time_total_ms = parsed["task_time_total_ms"]
    try: default_cores = int(spark_props.get("spark.executor.cores")) if spark_props.get("spark.executor.cores") else 1
    except Exception: default_cores = 1
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
    vcore_seconds = 0.0; mem_gb_seconds = 0.0
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
    ap = argparse.ArgumentParser(description="Read Spark event log from GCS and print metrics (app/job/stage/SQL) as JSON.")
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

        # Shape final SQL executions (flatten nodes; prefer final plan if present)
        sql_executions = []
        for exec_id, rec in sorted(parsed["sql_execs"].items()):
            nodes = rec["final_plan_nodes"] if rec.get("final_plan_nodes") else rec["initial_plan_nodes"]
            # Make nodes compact: keep name, simpleString, metrics
            compact_nodes = [{
                "node_index": n["node_index"],
                "parent_index": n["parent_index"],
                "nodeName": n.get("nodeName"),
                "simpleString": n.get("simpleString"),
                "metrics": n.get("metrics", {})
            } for n in nodes]
            sql_executions.append({
                "execution_id": exec_id,
                "description": rec.get("description"),
                "duration_ms": rec.get("duration_ms"),
                "details": rec.get("details"),             # may contain logical/optimized plans if explainMode extended/formatted
                "plan_nodes": compact_nodes                 # physical (or final AQE) plan nodes + metrics
            })

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
            "stages": sorted(parsed["stages"], key=lambda s: (s["stage_duration_ms"] is None, -(s["stage_duration_ms"] or 0), s["stage_id"], s["attempt"])),
            "sql_executions": sql_executions
        }

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

        print(json.dumps(out, indent=2))

    except Exception as e:
        print(json.dumps({"error": f"Failed to read/parse event log: {str(e)}"}))
        sys.exit(1)

if __name__ == "__main__":
    main()
