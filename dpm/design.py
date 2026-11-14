"""Medallion Architecture Mapping & Data Contracts Generator (Design Phase).

Categorizes pipeline tables into:
- Bronze: Raw immutable source ingestions
- Silver: Cleansed, conformed, and enriched intermediate staging
- Gold: Business-level aggregated terminal data marts

Generates deterministic data contracts with primary key and type assertions.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any


def assign_medallion_layers(jobs: list[dict[str, Any]]) -> dict[str, str]:
    """Assigns each table to bronze, silver, or gold."""
    all_inputs = set()
    all_outputs = set()

    for j in jobs:
        for inp in j.get("inputs", []):
            all_inputs.add(inp)
        for out in j.get("outputs", []):
            all_outputs.add(out)

    layers = {}
    # Terminal tables: produced but never consumed as inputs by other jobs -> Gold
    # Intermediate tables: produced and consumed -> Silver
    # Pure inputs: consumed but never produced -> Bronze
    for tbl in all_inputs:
        if tbl not in all_outputs:
            layers[tbl] = "bronze"
        else:
            layers[tbl] = "silver"

    for tbl in all_outputs:
        if tbl not in layers:
            layers[tbl] = "gold"

    return layers


def generate_data_contract(
    table_name: str,
    layer: str,
    columns: dict[str, str],
    primary_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Generates a formal data contract specification for a Medallion table."""
    pk = primary_keys or [list(columns.keys())[0]] if columns else ["id"]
    assertions = [
        f"non_null_primary_keys ({', '.join(pk)})",
        "schema_data_types_conformant",
        "acid_delta_transaction_enforced",
    ]
    numeric_cols = [c for c, t in columns.items() if any(k in t.lower() for k in ["int", "decimal", "float", "double"])]
    if numeric_cols:
        assertions.append(f"numeric_bounds_validated ({', '.join(numeric_cols[:3])})")

    return {
        "contract_id": f"CONTRACT_{table_name.upper()}_001",
        "table_name": table_name,
        "layer": layer.capitalize(),
        "status": "ACTIVE_VERIFIED",
        "primary_keys": pk,
        "columns": columns,
        "assertions": assertions,
    }

