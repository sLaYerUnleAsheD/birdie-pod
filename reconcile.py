"""Command-Line & Programmatic Entry Point for Reconciliation Validator.

Usage:
    python reconcile.py [--legacy legacy.csv] [--spark spark.csv] [--pk claim_id]
"""

from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from lakehouse_adapter.reconcile import reconcile_dataframes, ReconciliationScorecard
from lakehouse_adapter.benchmark_catalog import get_all_jobs


def run_benchmark_verification():
    print("=" * 72)
    print("  EXL Hackathon Challenge 03: Parallel-Run Output Reconciliation")
    print("  4-Check Parity Suite (Row Counts, Schema, Control Totals, Cell Diffs)")
    print("=" * 72)

    jobs = get_all_jobs()
    all_passed = True

    for j in jobs:
        score = j["scorecard"]
        status = "PASSED [100% PARITY]" if score["overall_passed"] else "FAILED"
        print(f"\n[JOB] {j['id']} ({j['dialect'].upper()} - {j['tier_code']}): {j['title']}")
        print(f"      Tier Routing: {j['tier']}")
        print(f"      Status: {status}")
        print(f"      - {score['check_1_row_count']['name']}: {'PASS' if score['check_1_row_count']['passed'] else 'FAIL'}")
        print(f"      - {score['check_2_schema']['name']}: {'PASS' if score['check_2_schema']['passed'] else 'FAIL'}")
        print(f"      - {score['check_3_control_totals']['name']}: {'PASS' if score['check_3_control_totals']['passed'] else 'FAIL'}")
        print(f"      - {score['check_4_cell_diff']['name']}: {'PASS' if score['check_4_cell_diff']['passed'] else 'FAIL'}")

        if not score["overall_passed"]:
            all_passed = False

    print("\n" + "=" * 72)
    if all_passed:
        print("  ALL 12 BENCHMARK JOBS PASSED 100% MATHEMATICAL RECONCILIATION!")
    else:
        print("  SOME RECONCILIATION CHECKS FAILED.")
    print("=" * 72)
    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel-run DataFrame reconciliation validator")
    parser.add_argument("--legacy", help="Path to legacy baseline CSV")
    parser.add_argument("--spark", help="Path to migrated PySpark output CSV")
    parser.add_argument("--pk", nargs="+", help="Primary key column(s) for record sorting")
    args = parser.parse_args()

    if not args.legacy or not args.spark:
        # Run built-in benchmark verification
        success = run_benchmark_verification()
        sys.exit(0 if success else 1)
    else:
        import csv
        with open(args.legacy, "r", encoding="utf-8") as f:
            legacy_data = list(csv.DictReader(f))
        with open(args.spark, "r", encoding="utf-8") as f:
            spark_data = list(csv.DictReader(f))

        scorecard = reconcile_dataframes(
            legacy_data=legacy_data,
            spark_data=spark_data,
            primary_keys=args.pk,
            job_id=Path(args.spark).stem
        )
        print(json.dumps(scorecard.to_dict(), indent=2))
        sys.exit(0 if scorecard.overall_passed else 1)

