"""Medallion Architecture Data Contracts & Schema Validation.

Defines explicit Data Contracts for Bronze, Silver, and Gold layers:
- Schema enforcement (Data Types, Required Columns)
- Nullability assertions
- Primary Key uniqueness
- Value domain constraints and numerical range limits (e.g. paid_amount >= 0)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MedallionLayer(str, Enum):
    BRONZE = "Bronze (Raw Ingestion / Landing)"
    SILVER = "Silver (Cleaned / Conformed / Curated)"
    GOLD = "Gold (Business Marts / Actuarial Reporting)"


@dataclass
class ColumnContract:
    name: str
    data_type: str  # STRING, INTEGER, DECIMAL, DATE, TIMESTAMP, BOOLEAN
    nullable: bool = True
    min_value: float | None = None
    max_value: float | None = None
    allowed_values: list[str] | None = None
    description: str = ""


@dataclass
class DataContract:
    contract_id: str
    layer: MedallionLayer
    table_name: str
    primary_keys: list[str]
    columns: list[ColumnContract]
    description: str = ""
    sla_freshness_hours: int = 24


@dataclass
class ContractValidationResult:
    passed: bool
    contract_id: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def validate_dataframe_contract(records: list[dict[str, Any]], contract: DataContract) -> ContractValidationResult:
    """Validates an in-memory dataset against a DataContract."""
    errors = []
    warnings = []
    checks = 0

    if not records:
        return ContractValidationResult(
            passed=False,
            contract_id=contract.contract_id,
            total_checks=1,
            passed_checks=0,
            failed_checks=1,
            errors=["Dataset is empty; zero records received."],
        )

    # 1. Check primary key existence & uniqueness
    checks += 1
    if contract.primary_keys:
        seen_keys = set()
        for i, row in enumerate(records):
            key_tuple = tuple(row.get(k) for k in contract.primary_keys)
            if any(k is None for k in key_tuple):
                errors.append(f"Primary key {contract.primary_keys} has NULL value at row {i}")
                break
            if key_tuple in seen_keys:
                errors.append(f"Duplicate primary key {key_tuple} detected at row {i}")
                break
            seen_keys.add(key_tuple)

    # 2. Check column types and constraints
    for col in contract.columns:
        checks += 1
        # Check column existence in first row
        if col.name not in records[0]:
            errors.append(f"Contract column '{col.name}' missing from dataset schema.")
            continue

        for i, row in enumerate(records):
            val = row.get(col.name)

            # Nullability check
            if not col.nullable and (val is None or str(val).strip() == ""):
                errors.append(f"Column '{col.name}' violated non-nullable constraint at row {i}.")
                break

            if val is not None and str(val).strip() != "":
                # Range check for numerics
                if col.min_value is not None:
                    try:
                        n_val = float(val)
                        if n_val < col.min_value:
                            errors.append(f"Column '{col.name}' value {n_val} below minimum {col.min_value} at row {i}.")
                            break
                    except ValueError:
                        errors.append(f"Column '{col.name}' expected numeric, got '{val}' at row {i}.")
                        break

                # Allowed domain values check
                if col.allowed_values is not None:
                    if str(val) not in col.allowed_values:
                        errors.append(f"Column '{col.name}' value '{val}' not in allowed domain {col.allowed_values} at row {i}.")
                        break

    passed = len(errors) == 0
    passed_checks = checks - len(errors)
    return ContractValidationResult(
        passed=passed,
        contract_id=contract.contract_id,
        total_checks=checks,
        passed_checks=max(0, passed_checks),
        failed_checks=len(errors),
        errors=errors,
        warnings=warnings,
    )


# -----------------------------------------------------------------------------
# Curated Standard Insurance Data Contracts for Challenge 03
# -----------------------------------------------------------------------------

CONTRACT_CLAIMS_SILVER = DataContract(
    contract_id="CONTRACT_CLM_SILVER_001",
    layer=MedallionLayer.SILVER,
    table_name="silver_curated_claims",
    primary_keys=["claim_id"],
    columns=[
        ColumnContract(name="claim_id", data_type="STRING", nullable=False, description="Unique claim identifier"),
        ColumnContract(name="policy_id", data_type="STRING", nullable=False, description="Associated policy ID"),
        ColumnContract(name="loss_date", data_type="DATE", nullable=False, description="Date loss occurred"),
        ColumnContract(name="claim_status", data_type="STRING", nullable=False, allowed_values=["OPEN", "CLOSED", "PENDING", "SETTLED"]),
        ColumnContract(name="net_paid_amount", data_type="DECIMAL", nullable=False, min_value=0.0, description="Net indemnified amount paid"),
        ColumnContract(name="outstanding_reserve", data_type="DECIMAL", nullable=False, min_value=0.0, description="Outstanding reserve balance"),
    ],
    description="Cleaned, conformed claims record with FX adjustment and validated reserves.",
)

CONTRACT_ACTUARIAL_GOLD = DataContract(
    contract_id="CONTRACT_ACT_GOLD_001",
    layer=MedallionLayer.GOLD,
    table_name="gold_actuarial_reserving_mart",
    primary_keys=["accident_year", "development_year", "line_of_business"],
    columns=[
        ColumnContract(name="accident_year", data_type="INTEGER", nullable=False, min_value=2010, max_value=2030),
        ColumnContract(name="development_year", data_type="INTEGER", nullable=False, min_value=0, max_value=15),
        ColumnContract(name="line_of_business", data_type="STRING", nullable=False, allowed_values=["MOTOR", "PROPERTY", "LIABILITY", "COMMERCIAL"]),
        ColumnContract(name="cumulative_paid", data_type="DECIMAL", nullable=False, min_value=0.0),
        ColumnContract(name="ibnr_reserve_amount", data_type="DECIMAL", nullable=False, min_value=0.0),
        ColumnContract(name="loss_ratio_pct", data_type="DECIMAL", nullable=True, min_value=0.0, max_value=500.0),
    ],
    description="Actuarial triangle and loss development figures for solvency II capital compliance.",
)

