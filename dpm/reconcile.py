"""Mathematical 4-Point Production Parity Reconciliation Engine.

Strict, exact reconciliation conforming to the Plan:
1. Decimal columns: exact Decimal equality after quantising to declared scale.
2. Control totals: summed as exact Decimal (no np.isclose float inaccuracies).
3. Float tolerance (rel 1e-9) ONLY for declared float columns.
4. Diff by primary key (outer merge logic): missing, extra, and changed rows reported.
5. No key declared: compared as multiset of row hashes.
6. Diagnostic feedback via as_repair_text(max_chars=2500).
7. Reports oracle_kind on every scorecard.
"""
from __future__ import annotations

import csv
import json
import math
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from dpm.tables import NULL_TOKEN, parse_type, quantize_decimal


@dataclass
class CheckSummary:
    name: str
    passed: bool
    details: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class Scorecard:
    job_id: str
    oracle_kind: str
    passed: bool
    match_percentage: float
    check_row_count: CheckSummary
    check_schema: CheckSummary
    check_control_totals: CheckSummary
    check_null_counts: CheckSummary
    check_key_uniqueness: CheckSummary
    check_cell_diff: CheckSummary
    discrepancies: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "oracle_kind": self.oracle_kind,
            "passed": self.passed,
            "match_percentage": self.match_percentage,
            "checks": {
                "row_count": {"passed": self.check_row_count.passed, "details": self.check_row_count.details},
                "schema": {"passed": self.check_schema.passed, "details": self.check_schema.details},
                "control_totals": {"passed": self.check_control_totals.passed, "details": self.check_control_totals.details},
                "null_counts": {"passed": self.check_null_counts.passed, "details": self.check_null_counts.details},
                "key_uniqueness": {"passed": self.check_key_uniqueness.passed, "details": self.check_key_uniqueness.details},
                "cell_diff": {"passed": self.check_cell_diff.passed, "details": self.check_cell_diff.details},
            },
            "discrepancies": self.discrepancies[:10],
        }

    def as_repair_text(self, max_chars: int = 2500) -> str:
        """Produce actionable diagnostic text for the agent repair loop."""
        if self.passed:
            return f"PASS: All reconciliation checks passed record-for-record (oracle_kind={self.oracle_kind})."

        lines = [
            f"RECONCILIATION FAILURE for job '{self.job_id}' (Oracle: {self.oracle_kind}):",
            f"Overall Status: FAILED ({self.match_percentage:.1f}% parity)",
        ]

        if not self.check_row_count.passed:
            lines.append(f"• Row Count Mismatch: {self.check_row_count.details}")
        if not self.check_schema.passed:
            lines.append(f"• Schema Discrepancy: {self.check_schema.details}")
        if not self.check_key_uniqueness.passed:
            lines.append(f"• Key Uniqueness Failure: {self.check_key_uniqueness.details}")
        if not self.check_control_totals.passed:
            lines.append(f"• Control Totals Drift: {self.check_control_totals.details}")
        if not self.check_null_counts.passed:
            lines.append(f"• Null Count Drift: {self.check_null_counts.details}")
        if not self.check_cell_diff.passed:
            lines.append(f"• Cell Parity Differences: {self.check_cell_diff.details}")

        if self.discrepancies:
            lines.append("\nFirst Sample Discrepancies (key, col, expected, actual):")
            for d in self.discrepancies[:10]:
                lines.append(f"  - Key: {d.get('key')} | Col: {d.get('column')} | Expected: {d.get('expected')!r} | Actual: {d.get('actual')!r}")

        full_text = "\n".join(lines)
        if len(full_text) > max_chars:
            return full_text[:max_chars - 20] + "\n... [truncated]"
        return full_text


def _to_decimal(val: Any) -> Decimal | None:
    if val is None or val == "" or val == NULL_TOKEN:
        return None
    try:
        return Decimal(str(val).strip())
    except (InvalidOperation, ValueError):
        return None


def read_csv_rows(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows = []
    with p.open(mode="r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cleaned = {}
            for k, v in row.items():
                cleaned[k] = None if v == NULL_TOKEN or v == r"\N" else v
            rows.append(cleaned)
    return rows


def reconcile(
    expected_rows: list[dict[str, Any]] | Path | str,
    actual_rows: list[dict[str, Any]] | Path | str,
    keys: list[str] | None = None,
    schema: dict[str, str] | None = None,
    oracle_kind: str = "sqlserver",
    job_id: str = "job",
) -> Scorecard:
    """Reconciles expected (oracle) vs actual (PySpark) tabular outputs."""
    if isinstance(expected_rows, (str, Path)):
        expected_rows = read_csv_rows(expected_rows)
    if isinstance(actual_rows, (str, Path)):
        actual_rows = read_csv_rows(actual_rows)

    keys = keys or []
    schema = schema or {}

    # 1. Row Count
    exp_len = len(expected_rows)
    act_len = len(actual_rows)
    row_count_passed = (exp_len == act_len)
    check_row_count = CheckSummary(
        name="Row Count",
        passed=row_count_passed,
        details=f"Expected: {exp_len} rows, Actual: {act_len} rows (Delta: {act_len - exp_len})",
        metrics={"expected": exp_len, "actual": act_len, "delta": act_len - exp_len},
    )

    # 2. Schema
    exp_cols = list(expected_rows[0].keys()) if expected_rows else list(schema.keys())
    act_cols = list(actual_rows[0].keys()) if actual_rows else []
    missing_cols = set(exp_cols) - set(act_cols)
    extra_cols = set(act_cols) - set(exp_cols)
    schema_passed = (not missing_cols and not extra_cols)
    schema_detail = "Schema match exact." if schema_passed else f"Missing: {list(missing_cols)}, Extra: {list(extra_cols)}"
    check_schema = CheckSummary(
        name="Schema Parity",
        passed=schema_passed,
        details=schema_detail,
        metrics={"missing_columns": list(missing_cols), "extra_columns": list(extra_cols)},
    )

    # 3. Key Uniqueness
    def make_key(row, key_cols):
        return tuple(str(row.get(k, "")) for k in key_cols)

    key_uniq_passed = True
    key_uniq_detail = "Keys unique."
    if keys:
        exp_keys = [make_key(r, keys) for r in expected_rows]
        act_keys = [make_key(r, keys) for r in actual_rows]
        exp_dups = len(exp_keys) - len(set(exp_keys))
        act_dups = len(act_keys) - len(set(act_keys))
        if exp_dups > 0 or act_dups > 0:
            key_uniq_passed = False
            key_uniq_detail = f"Key duplicates detected: {exp_dups} in expected, {act_dups} in actual"
    check_key_uniqueness = CheckSummary(
        name="Key Uniqueness",
        passed=key_uniq_passed,
        details=key_uniq_detail,
    )

    # 4. Control Totals (Exact Decimal Summation)
    shared_cols = [c for c in exp_cols if c in act_cols]
    totals_passed = True
    totals_diffs = []
    
    for col in shared_cols:
        col_type = schema.get(col, "string").lower()
        if col_type.startswith("decimal") or col_type in ("int", "bigint"):
            exp_sum = Decimal(0)
            act_sum = Decimal(0)
            for r in expected_rows:
                d = _to_decimal(r.get(col))
                if d is not None:
                    exp_sum += d
            for r in actual_rows:
                d = _to_decimal(r.get(col))
                if d is not None:
                    act_sum += d
            if exp_sum != act_sum:
                totals_passed = False
                totals_diffs.append(f"{col}: expected sum {exp_sum}, actual sum {act_sum} (diff {act_sum - exp_sum})")
        elif col_type == "float":
            f_exp_sum = sum(float(r.get(col) or 0.0) for r in expected_rows if r.get(col) is not None)
            f_act_sum = sum(float(r.get(col) or 0.0) for r in actual_rows if r.get(col) is not None)
            rel_err = abs(f_exp_sum - f_act_sum) / max(abs(f_exp_sum), 1e-9)
            if rel_err > 1e-9:
                totals_passed = False
                totals_diffs.append(f"{col} (float): expected {f_exp_sum}, actual {f_act_sum} (rel_err={rel_err:.2e})")

    check_control_totals = CheckSummary(
        name="Control Totals",
        passed=totals_passed,
        details="All numeric control totals match exact Decimal." if totals_passed else "; ".join(totals_diffs[:3]),
    )

    # 5. Null Counts
    nulls_passed = True
    null_diffs = []
    for col in shared_cols:
        exp_nulls = sum(1 for r in expected_rows if r.get(col) is None or r.get(col) == NULL_TOKEN)
        act_nulls = sum(1 for r in actual_rows if r.get(col) is None or r.get(col) == NULL_TOKEN)
        if exp_nulls != act_nulls:
            nulls_passed = False
            null_diffs.append(f"{col}: expected {exp_nulls} nulls, actual {act_nulls} nulls")
    check_null_counts = CheckSummary(
        name="Null Counts",
        passed=nulls_passed,
        details="Null count distribution identical across all columns." if nulls_passed else "; ".join(null_diffs[:3]),
    )

    # 6. Cell Diff (Outer Merge on Primary Key or Row Hash Multiset)
    discrepancies: list[dict[str, Any]] = []
    total_comparisons = 0
    matched_comparisons = 0

    if keys:
        exp_map: dict[tuple, dict] = {}
        for r in expected_rows:
            exp_map[make_key(r, keys)] = r
        act_map: dict[tuple, dict] = {}
        for r in actual_rows:
            act_map[make_key(r, keys)] = r

        all_keys = sorted(list(set(exp_map.keys()) | set(act_map.keys())))
        missing_rows_count = 0
        extra_rows_count = 0

        for k in all_keys:
            in_exp = k in exp_map
            in_act = k in act_map
            if in_exp and not in_act:
                missing_rows_count += 1
                discrepancies.append({"key": str(k), "column": "*row*", "expected": "present", "actual": "missing"})
            elif in_act and not in_exp:
                extra_rows_count += 1
                discrepancies.append({"key": str(k), "column": "*row*", "expected": "missing", "actual": "present"})
            else:
                erow = exp_map[k]
                arow = act_map[k]
                for col in shared_cols:
                    total_comparisons += 1
                    col_type = schema.get(col, "string").lower()
                    eval_ = erow.get(col)
                    aval_ = arow.get(col)

                    match = False
                    if eval_ is None and aval_ is None:
                        match = True
                    elif eval_ is None or aval_ is None:
                        match = False
                    elif col_type.startswith("decimal"):
                        try:
                            # Quantize to declared scale
                            _, params = parse_type(col_type)
                            scale = params[1] if len(params) > 1 else 2
                            match = (quantize_decimal(eval_, scale) == quantize_decimal(aval_, scale))
                        except Exception:
                            match = (str(eval_).strip() == str(aval_).strip())
                    elif col_type == "float":
                        try:
                            fe = float(eval_)
                            fa = float(aval_)
                            match = abs(fe - fa) <= max(abs(fe), 1e-9) * 1e-9
                        except Exception:
                            match = (str(eval_) == str(aval_))
                    else:
                        match = (str(eval_).strip() == str(aval_).strip())

                    if match:
                        matched_comparisons += 1
                    else:
                        discrepancies.append({
                            "key": str(k),
                            "column": col,
                            "expected": eval_,
                            "actual": aval_,
                        })

        cell_passed = (len(discrepancies) == 0 and missing_rows_count == 0 and extra_rows_count == 0)
        cell_details = (
            f"100% record-for-record exact parity across {total_comparisons} cells."
            if cell_passed
            else f"Discrepancies found: {len(discrepancies)} cells, {missing_rows_count} missing rows, {extra_rows_count} extra rows"
        )
    else:
        # Multiset row hash comparison
        def row_sig(r):
            return tuple(sorted((k, str(v).strip() if v is not None else NULL_TOKEN) for k, v in r.items()))

        exp_counts = Counter(row_sig(r) for r in expected_rows)
        act_counts = Counter(row_sig(r) for r in actual_rows)

        diff_count = 0
        all_sigs = set(exp_counts.keys()) | set(act_counts.keys())
        for sig in all_sigs:
            ec = exp_counts.get(sig, 0)
            ac = act_counts.get(sig, 0)
            if ec != ac:
                diff_count += abs(ec - ac)
                discrepancies.append({"key": "hash_sig", "column": "*row*", "expected": f"count={ec}", "actual": f"count={ac}"})

        cell_passed = (diff_count == 0)
        total_comparisons = max(exp_len, 1)
        matched_comparisons = total_comparisons - diff_count if cell_passed else 0
        cell_details = "Multiset row equality verified 100%." if cell_passed else f"{diff_count} row differences in multiset hash check."

    check_cell_diff = CheckSummary(
        name="Record-for-Record Cell Diff",
        passed=cell_passed,
        details=cell_details,
        metrics={"total_cells": total_comparisons, "discrepancies": len(discrepancies)},
    )

    match_pct = 100.0 if (total_comparisons == 0 and exp_len == act_len) else (
        100.0 if cell_passed and row_count_passed else max(0.0, round((matched_comparisons / max(total_comparisons, 1)) * 100.0, 2))
    )

    overall_passed = (
        row_count_passed
        and schema_passed
        and totals_passed
        and nulls_passed
        and key_uniq_passed
        and cell_passed
    )

    return Scorecard(
        job_id=job_id,
        oracle_kind=oracle_kind,
        passed=overall_passed,
        match_percentage=match_pct,
        check_row_count=check_row_count,
        check_schema=check_schema,
        check_control_totals=check_control_totals,
        check_null_counts=check_null_counts,
        check_key_uniqueness=check_key_uniqueness,
        check_cell_diff=check_cell_diff,
        discrepancies=discrepancies,
    )
