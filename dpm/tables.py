"""Shared CSV + logical-type helpers.

Null and empty string are different values. CSV uses the token ``\\N`` for null
and an empty field for an empty string, so both survive a round trip.
"""
from __future__ import annotations

import csv
import json
import re
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

NULL_TOKEN = r"\N"

_DECIMAL = re.compile(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.I)
_CHAR = re.compile(r"char\s*\(\s*(\d+)\s*\)", re.I)


def parse_type(type_name: str) -> tuple[str, tuple]:
    """Return (base, params). ``decimal(12,2)`` -> ('decimal', (12, 2))."""
    text = (type_name or "").strip().lower()
    m = _DECIMAL.fullmatch(text.replace(" ", ""))
    if m:
        return "decimal", (int(m.group(1)), int(m.group(2)))
    m = _CHAR.fullmatch(text.replace(" ", ""))
    if m:
        return "char", (int(m.group(1)),)
    bases = {"int", "bigint", "float", "string", "date", "timestamp", "bool"}
    if text not in bases:
        raise ValueError(f"unknown logical type {type_name!r}")
    return text, ()


def base_type(type_name: str) -> str:
    return parse_type(type_name)[0]


def spark_type(type_name: str) -> str:
    base, params = parse_type(type_name)
    if base == "decimal":
        return f"DECIMAL({params[0]},{params[1]})"
    if base == "char":
        return "STRING"
    return {
        "int": "INT",
        "bigint": "BIGINT",
        "float": "DOUBLE",
        "string": "STRING",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bool": "BOOLEAN",
    }[base]


def sqlserver_type(type_name: str) -> str:
    base, params = parse_type(type_name)
    if base == "decimal":
        return f"DECIMAL({params[0]},{params[1]})"
    if base == "char":
        return f"CHAR({params[0]})"
    return {
        "int": "INT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "string": "NVARCHAR(200)",
        "date": "DATE",
        "timestamp": "DATETIME2",
        "bool": "BIT",
    }[base]


def logical_from_spark(dtype: str) -> str:
    text = (dtype or "").strip().lower()
    m = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", text)
    if m:
        return f"decimal({m.group(1)},{m.group(2)})"
    return {
        "int": "int",
        "integer": "int",
        "bigint": "bigint",
        "long": "bigint",
        "double": "float",
        "float": "float",
        "string": "string",
        "date": "date",
        "timestamp": "timestamp",
        "boolean": "bool",
        "bool": "bool",
    }.get(text, text)


def quantize_decimal(value, scale: int) -> Decimal:
    quantum = Decimal(1).scaleb(-scale)
    return Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP)


def write_rows(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            out = {}
            for col in columns:
                val = row.get(col)
                out[col] = NULL_TOKEN if val is None else val
            writer.writerow(out)


def write_schema(path: Path, columns: dict[str, str]) -> None:
    payload = {
        "columns": [{"name": name, "logical": logical} for name, logical in columns.items()],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_schema(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {c["name"]: c["logical"] for c in payload["columns"]}
