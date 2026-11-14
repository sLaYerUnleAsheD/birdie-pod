"""Benchmark Runner across Providers, Samples, and Force-Agent Baselines.

Supports:
- Running all 12 benchmark jobs (8 T-SQL, 4 SAS)
- Cost capping ($5.00 limit)
- Deterministic-first route vs --force-agent baseline
- Summary aggregation
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import Any

from dpm.jobload import load_job, list_jobs
from dpm.pipeline import migrate_job, MigrationResult


def run_benchmark(
    jobs_dir: str | Path = "dpm/jobs",
    force_agent: bool = False,
    cost_cap: float = 5.0,
) -> dict[str, Any]:
    """Runs all benchmark jobs and generates an aggregated scorecard."""
    job_ids = list_jobs(jobs_dir)
    results: list[MigrationResult] = []
    total_cost = 0.0

    print("=" * 72)
    print("  Aegis Data Platform Modernization Benchmark Suite")
    print(f"  Total Jobs: {len(job_ids)} | Force-Agent Baseline: {force_agent}")
    print("=" * 72)

    for jid in job_ids:
        if total_cost >= cost_cap:
            print(f"[BUDGET EXCEEDED] Halting benchmark at ${total_cost:.4f} cap.")
            break

        loaded = load_job(jid, jobs_dir=jobs_dir)
        meta = loaded.meta
        src = loaded.source

        res = migrate_job(
            job_id=jid,
            source_code=src,
            meta=meta,
            expected_data=[],
            force_agent=force_agent,
        )
        results.append(res)
        total_cost += res.total_cost_usd

        print(f"  [{res.status.upper()}] {jid:10} | {res.tier[:15]} | Route: {res.route_path:10} | Cost: ${res.total_cost_usd:.4f}")

    verified = sum(1 for r in results if r.status == "verified")
    zero_cost_count = sum(1 for r in results if r.total_cost_usd == 0.0)

    summary = {
        "total_jobs": len(job_ids),
        "executed": len(results),
        "verified": verified,
        "failed": len(results) - verified,
        "zero_cost_count": zero_cost_count,
        "deterministic_share": f"{(zero_cost_count / max(len(results), 1)) * 100:.1f}%",
        "total_cost_usd": round(total_cost, 4),
        "results": [r.to_dict() for r in results],
    }

    print("=" * 72)
    print(f"  BENCHMARK COMPLETE: {verified}/{len(results)} Verified | {summary['deterministic_share']} $0 Deterministic")
    print("=" * 72)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aegis Modernize Benchmark Runner")
    parser.add_argument("--force-agent", action="store_true", help="Force agent translation skipping deterministic routing")
    parser.add_argument("--cost-cap", type=float, default=5.0, help="Maximum spend cap in USD")
    args = parser.parse_args()

    run_benchmark(force_agent=args.force_agent, cost_cap=args.cost_cap)
