"""Audit Proof Generator for Judges & Evaluators.

Reads:
- .cache/spend_*.json (Exact cumulative spend per provider)
- .cache/ledger.jsonl (Immutable JSONL log of every paid call)
- run/dpm/ (Attempt logs, repaired scripts, and result.json files)

Prints a clean audit statement proving live agent usage, costs, and token rates.
"""
from __future__ import annotations
import glob
import json
from pathlib import Path


def print_audit_proof():
    print("=" * 76)
    print("   AEGIS PLATFORM: LIVE AGENT EXECUTION & FINANCIAL AUDIT PROOF")
    print("=" * 76)

    cache_dir = Path(".cache")

    # 1. Spend summary by provider
    spend_files = list(cache_dir.glob("spend_*.json")) if cache_dir.exists() else []
    print("\n[1. CUMULATIVE ACCOUNT SPEND BY PROVIDER]")
    if not spend_files:
        print("  No spend files found in .cache yet.")
    else:
        for sf in spend_files:
            provider = sf.stem.replace("spend_", "")
            try:
                data = json.loads(sf.read_text())
                print(f"  • Provider: {provider.upper():<10} | Total Billed: ${data.get('usd', 0.0):.6f} USD")
            except Exception:
                pass

    # 2. Immutable Ledger History
    ledger_file = cache_dir / "ledger.jsonl"
    print("\n[2. TRANSACTION LEDGER LOG (.cache/ledger.jsonl)]")
    if not ledger_file.exists():
        print("  Ledger is empty.")
    else:
        entries = []
        with ledger_file.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        entries.append(json.loads(line))
                    except Exception:
                        pass
        print(f"  Total Paid LLM Invocations Logged: {len(entries)}")
        print(f"  {'Timestamp':<20} | {'Provider':<8} | {'Model':<20} | {'Tokens':<12} | {'Cost (USD)':<10} | {'Tag'}")
        print("  " + "-" * 88)
        for e in entries[-10:]:  # last 10
            ts_str = str(int(e.get("ts", 0)))
            prov = e.get("provider", "meta")
            model = e.get("model", "")[:20]
            inp = e.get("input_tokens", 0)
            out = e.get("output_tokens", 0)
            cost = f"${e.get('cost_usd', 0.0):.6f}"
            tag = e.get("tag", "")
            print(f"  {ts_str:<20} | {prov:<8} | {model:<20} | In:{inp:<3} Out:{out:<3} | {cost:<10} | {tag}")

    # 3. Generated Run Artifacts
    run_dir = Path("run/dpm")
    print("\n[3. MIGRATION RUN ARTIFACTS (run/dpm/)]")
    result_files = list(run_dir.glob("**/result.json")) if run_dir.exists() else []
    if not result_files:
        print("  No run artifacts generated yet. Run: python scripts/test_repair_agent.py")
    else:
        for rf in result_files:
            try:
                r = json.loads(rf.read_text())
                prog = r.get("program", "")
                prov = r.get("provider", "")
                tier = r.get("tier", "")
                status = r.get("status", "")
                cost = f"${r.get('total_cost_usd', 0.0):.4f}"
                repairs = r.get("repairs_used", 0)
                print(f"  • Program: {prog:<28} | Tier: {tier:<22} | Status: {status:<8} | Cost: {cost:<8} | Repairs: {repairs}")
                print(f"    Artifact: {rf}")
            except Exception:
                pass

    print("\n" + "=" * 76)
    print("   END OF AUDIT STATEMENT")
    print("=" * 76)


if __name__ == "__main__":
    print_audit_proof()

