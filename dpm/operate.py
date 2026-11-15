"""Operational Orchestration, OpenLineage Export, SLA Monitor & Approval Gate.

Implements:
1. workflow.json generator: Directed Acyclic Graph (DAG) with cron, SLA minutes, and depends_on.
2. OpenLineage standard facet JSON generator.
3. Human Approval Gate: Reads approvals.json (defaults to false).
   Refuses to publish or cut over unapproved migration jobs.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any

from dpm.discover import build_waves


def generate_workflow_dag(jobs: list[dict[str, Any]], waves: list[list[str]] | None = None) -> dict[str, Any]:
    """Builds workflow.json with dependencies derived from topological waves."""
    if not waves:
        waves = build_waves(jobs)

    job_to_wave = {}
    for w_idx, wave in enumerate(waves):
        for jid in wave:
            job_to_wave[jid] = w_idx

    tasks = []
    job_map = {j["id"]: j for j in jobs}

    for jid, wave_idx in job_to_wave.items():
        j = job_map.get(jid, {})
        # Depends on all jobs in the immediately preceding wave that produce this job's inputs
        deps = []
        if wave_idx > 0:
            prev_wave_jobs = waves[wave_idx - 1]
            deps = [p for p in prev_wave_jobs if any(out in j.get("inputs", []) for out in job_map.get(p, {}).get("outputs", []))]

        tasks.append({
            "task_id": f"task_{jid}",
            "job_id": jid,
            "dialect": j.get("dialect", "tsql"),
            "tier": j.get("tier_code", "A"),
            "depends_on": deps,
            "sla_minutes": 15 if j.get("tier_code") == "A" else 30,
            "retries": 2,
            "timeout_seconds": 1800,
        })

    return {
        "workflow_id": "aegis_modernize_daily_batch",
        "description": "Orchestrated Azure Databricks Delta Lake batch processing pipeline.",
        "schedule": {
            "cron": "0 2 * * *",
            "timezone": "UTC",
        },
        "default_sla_minutes": 60,
        "tasks": tasks,
    }


def generate_openlineage_event(job: dict[str, Any]) -> dict[str, Any]:
    """Generates OpenLineage run event specification."""
    jid = job["id"]
    inputs = [{"namespace": "azure.lakehouse.delta", "name": f"bronze.{tbl}"} for tbl in job.get("inputs", [])]
    outputs = [{"namespace": "azure.lakehouse.delta", "name": f"gold.{tbl}"} for tbl in job.get("outputs", [])]

    return {
        "eventType": "COMPLETE",
        "job": {
            "namespace": "aegis.migrate",
            "name": jid,
            "facets": {
                "tier": {"_producer": "aegis-router", "value": job.get("tier_code", "A")},
                "oracle_kind": {"_producer": "aegis-oracle", "value": job.get("oracle_kind", "sqlserver")},
            }
        },
        "inputs": inputs,
        "outputs": outputs,
        "producer": "https://github.com/sLaYerUnleAsheD/birdie-pod",
    }


class ApprovalGateError(RuntimeError):
    pass


def check_approval_gate(job_id: str, approvals_path: str | Path = "approvals.json") -> bool:
    """Verifies whether a human operator has explicitly approved publishing the job.
    Defaults to False. Refuses publication if unapproved.
    """
    p = Path(approvals_path)
    if not p.exists():
        # Initialize default unapproved template
        p.write_text(json.dumps({job_id: False}, indent=2), encoding="utf-8")
        raise ApprovalGateError(
            f"Approval gate BLOCKED: '{job_id}' is not approved. Created {p.name} with default 'false'. A human sign-off is required."
        )

    approvals = json.loads(p.read_text(encoding="utf-8"))
    approved = approvals.get(job_id, False)
    if not approved:
        raise ApprovalGateError(
            f"Approval gate BLOCKED: Cutover of '{job_id}' rejected. Human approval in {p.name} is 'false'."
        )
    return True

