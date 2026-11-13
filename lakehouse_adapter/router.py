"""3-Tier Routing Engine for Legacy T-SQL and SAS Migration.

Implements the deterministic AST vs LLM triage architecture:
For T-SQL (.sql):
  - Tier A (Easy): Standard SELECT / JOIN / GROUP BY -> sqlglot only ($0 token cost, 100% deterministic)
  - Tier B (Medium): CTEs, OVER (PARTITION BY ...), PIVOT -> sqlglot transpiles skeleton first,
                    then Meta models refine/wrap into clean PySpark DataFrame code with lineage comments.
  - Tier C (Hard): Procedural T-SQL (DECLARE @var, WHILE, CURSOR, #temp_tables, MERGE, EXEC) -> 100% LLM Agent.

For SAS (.sas):
  - Tier A (Easy): PROC SQL only -> strip PROC SQL; / QUIT; wrapper and pass inner SQL to sqlglot ($0 token cost).
  - Tier B (Medium): DATA steps + PROC SORT / PROC MEANS / PROC FORMAT -> Route to Meta LLM.
  - Tier C (Hard): RETAIN, FIRST./LAST., %MACRO loops, MERGE with IN= flags -> Route to Meta LLM with self-healing agentic loop.
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class Dialect(str, Enum):
    TSQL = "tsql"
    SAS = "sas"
    UNKNOWN = "unknown"


class Tier(str, Enum):
    TIER_A = "Tier A (Easy - Deterministic / $0 Cost)"
    TIER_B = "Tier B (Medium - Hybrid AST + Meta LLM)"
    TIER_C = "Tier C (Hard - Autonomous Meta Agent Loop)"


@dataclass
class RoutingDecision:
    dialect: Dialect
    tier: Tier
    tier_code: str  # "A", "B", "C"
    cost_estimate_usd: float
    reason: str
    detected_features: list[str] = field(default_factory=list)
    inner_sql: str | None = None
    use_sqlglot_ast: bool = False
    use_llm_agent: bool = False
    requires_repair_loop: bool = False


# T-SQL Detection Patterns
TSQL_PROCEDURAL_PATTERNS = [
    (r"\bDECLARE\s+@\w+", "DECLARE variable"),
    (r"\bWHILE\b", "WHILE loop"),
    (r"\bCURSOR\b", "CURSOR iteration"),
    (r"#\w+", "#temp_table usage"),
    (r"##\w+", "##global_temp_table usage"),
    (r"\bMERGE\s+INTO\b", "MERGE statement"),
    (r"\bEXEC(?:UTE)?\s+", "EXEC dynamic/stored proc"),
    (r"\bBEGIN\s+TRAN(?:SACTION)?\b", "Transaction management"),
    (r"\bTRY\s+BEGIN\b|\bBEGIN\s+CATCH\b", "TRY/CATCH block"),
]

TSQL_COMPLEX_PATTERNS = [
    (r"\bWITH\s+[a-zA-Z0-9_]+\s+AS\s*\(", "Common Table Expression (CTE)"),
    (r"\bOVER\s*\(\s*(?:PARTITION\s+BY|ORDER\s+BY)", "Window function OVER(...)"),
    (r"\bPIVOT\b|\bUNPIVOT\b", "PIVOT/UNPIVOT operator"),
    (r"\bCROSS\s+APPLY\b|\bOUTER\s+APPLY\b", "APPLY operator"),
    (r"\bROLLUP\b|\bCUBE\b", "GROUP BY ROLLUP/CUBE"),
]

# SAS Detection Patterns
SAS_HARD_PATTERNS = [
    (r"\bRETAIN\b", "RETAIN accumulator/stateful variable"),
    (r"\bFIRST\.\w+|\bLAST\.\w+", "FIRST./LAST. by-group processing"),
    (r"%MACRO\b|\b%MEND\b", "SAS %MACRO definition"),
    (r"\bMERGE\b[^\;]+\bIN\s*=", "MERGE with IN= tracking flags"),
    (r"\bARRAY\b[^\;]+(?:_TEMPORARY_|DIM)", "Multi-dimensional or temporary ARRAY"),
    (r"\bDO\s+WHILE\b|\bDO\s+UNTIL\b", "DO WHILE / UNTIL loops"),
]

SAS_MEDIUM_PATTERNS = [
    (r"\bDATA\s+\w+", "SAS DATA step"),
    (r"\bPROC\s+SORT\b", "PROC SORT step"),
    (r"\bPROC\s+MEANS\b|\bPROC\s+SUMMARY\b", "PROC MEANS/SUMMARY aggregation"),
    (r"\bPROC\s+FORMAT\b", "PROC FORMAT custom mapping"),
    (r"\bPROC\s+TRANSPOSE\b", "PROC TRANSPOSE reshaping"),
    (r"\bSET\b[^\;]+", "SET concatenation"),
]


def detect_dialect(source: str, filename: str = "") -> Dialect:
    fn = filename.lower()
    if fn.endswith(".sql"):
        return Dialect.TSQL
    if fn.endswith(".sas"):
        return Dialect.SAS

    # Heuristic detection from code content
    if re.search(r"\bPROC\s+(?:SQL|SORT|MEANS|SUMMARY|FORMAT|TRANSPOSE)\b|\bDATA\s+\w+\s*;", source, re.IGNORECASE):
        return Dialect.SAS
    if re.search(r"\bSELECT\b.*\bFROM\b|\bCREATE\s+(?:TABLE|VIEW|PROCEDURE)\b", source, re.IGNORECASE):
        return Dialect.TSQL
    return Dialect.UNKNOWN


def route_tsql(source: str) -> RoutingDecision:
    # 1. Check for Procedural features -> Tier C
    detected_c = []
    for pat, name in TSQL_PROCEDURAL_PATTERNS:
        if re.search(pat, source, re.IGNORECASE):
            detected_c.append(name)

    if detected_c:
        return RoutingDecision(
            dialect=Dialect.TSQL,
            tier=Tier.TIER_C,
            tier_code="C",
            cost_estimate_usd=0.045,
            reason="Contains multi-step procedural T-SQL syntax that AST cannot directly transpile to single DataFrame.",
            detected_features=detected_c,
            use_sqlglot_ast=False,
            use_llm_agent=True,
            requires_repair_loop=True,
        )

    # 2. Check for Complex declarative SQL -> Tier B
    detected_b = []
    for pat, name in TSQL_COMPLEX_PATTERNS:
        if re.search(pat, source, re.IGNORECASE):
            detected_b.append(name)

    if detected_b:
        return RoutingDecision(
            dialect=Dialect.TSQL,
            tier=Tier.TIER_B,
            tier_code="B",
            cost_estimate_usd=0.015,
            reason="Contains advanced declarative SQL (CTEs, Windowing, Pivot). Transpiles via sqlglot first, then Meta model generates clean PySpark DataFrame API.",
            detected_features=detected_b,
            use_sqlglot_ast=True,
            use_llm_agent=True,
            requires_repair_loop=False,
        )

    # 3. Standard declarative SQL -> Tier A
    return RoutingDecision(
        dialect=Dialect.TSQL,
        tier=Tier.TIER_A,
        tier_code="A",
        cost_estimate_usd=0.000,
        reason="Standard SELECT/JOIN/GROUP BY logic. Handled 100% deterministically by sqlglot AST transpiler with $0 token cost.",
        detected_features=["Standard SELECT", "Relational JOIN", "GROUP BY/Aggregation"],
        use_sqlglot_ast=True,
        use_llm_agent=False,
        requires_repair_loop=False,
    )


def route_sas(source: str) -> RoutingDecision:
    # Strip comments for cleaner scan
    cleaned = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    cleaned = re.sub(r"^\s*\*.*?;", "", cleaned, flags=re.MULTILINE)

    # Check for Easy SAS: PROC SQL only
    has_proc_sql = bool(re.search(r"\bPROC\s+SQL\b", cleaned, re.IGNORECASE))
    has_data_step = bool(re.search(r"\bDATA\s+\w+\s*;", cleaned, re.IGNORECASE))
    has_macro = bool(re.search(r"%MACRO\b", cleaned, re.IGNORECASE))
    has_other_proc = bool(re.search(r"\bPROC\s+(?!SQL\b)\w+", cleaned, re.IGNORECASE))

    if has_proc_sql and not has_data_step and not has_macro and not has_other_proc:
        # Extract the inner SQL
        match = re.search(r"\bPROC\s+SQL\s*;\s*(.*?)\s*\bQUIT\s*;", cleaned, re.IGNORECASE | re.DOTALL)
        inner_sql = match.group(1).strip() if match else cleaned
        return RoutingDecision(
            dialect=Dialect.SAS,
            tier=Tier.TIER_A,
            tier_code="A",
            cost_estimate_usd=0.000,
            reason="Pure PROC SQL script with no DATA steps or macros. Strips SAS wrapper and translates deterministically via sqlglot.",
            detected_features=["PROC SQL only", "No DATA step", "No Macro"],
            inner_sql=inner_sql,
            use_sqlglot_ast=True,
            use_llm_agent=False,
            requires_repair_loop=False,
        )

    # Check for Hard SAS
    detected_c = []
    for pat, name in SAS_HARD_PATTERNS:
        if re.search(pat, cleaned, re.IGNORECASE):
            detected_c.append(name)

    if detected_c:
        return RoutingDecision(
            dialect=Dialect.SAS,
            tier=Tier.TIER_C,
            tier_code="C",
            cost_estimate_usd=0.055,
            reason="Contains complex stateful SAS syntax (RETAIN, FIRST./LAST., %MACRO, MERGE IN=). Routes to Meta LLM with multi-step self-healing loop.",
            detected_features=detected_c,
            use_sqlglot_ast=False,
            use_llm_agent=True,
            requires_repair_loop=True,
        )

    # Check for Medium SAS
    detected_b = []
    for pat, name in SAS_MEDIUM_PATTERNS:
        if re.search(pat, cleaned, re.IGNORECASE):
            detected_b.append(name)

    return RoutingDecision(
        dialect=Dialect.SAS,
        tier=Tier.TIER_B,
        tier_code="B",
        cost_estimate_usd=0.020,
        reason="Standard SAS DATA steps and PROCs (SORT, MEANS, FORMAT). Routes directly to Meta LLM with structured PySpark prompt template.",
        detected_features=detected_b or ["Standard DATA step"],
        use_sqlglot_ast=False,
        use_llm_agent=True,
        requires_repair_loop=False,
    )


def route_legacy_script(source: str, filename: str = "") -> RoutingDecision:
    dialect = detect_dialect(source, filename)
    if dialect == Dialect.TSQL:
        return route_tsql(source)
    elif dialect == Dialect.SAS:
        return route_sas(source)
    else:
        # Fallback to T-SQL heuristic
        return route_tsql(source)

