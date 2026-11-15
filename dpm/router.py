"""Static & Lexical Router for T-SQL and SAS Programs to PySpark Tiers.

Tier Assignment Rules:
- T-SQL:
  * Tier C (Agent Only): DECLARE @, @var, WHILE, CURSOR, EXEC / sp_executesql,
                         MERGE, #temp / ##temp, BEGIN...END, exp.Command, parse error.
  * Tier B (Hybrid AST + Meta LLM): CTE (WITH ... AS), Window functions (OVER),
                         PIVOT / UNPIVOT, CROSS / OUTER APPLY, subquery depth > 2.
  * Tier A (Deterministic AST): Pure relational queries (SELECT, JOIN, WHERE, GROUP BY, ORDER BY).

- SAS:
  * Tier A (Deterministic): Pure PROC SQL ... QUIT; with no DATA step, %macros,
                            calculated, monotonic, intnx, put(, input(.
  * Tier B (Agent Single-Shot): DATA step, PROC SORT NODUPKEY, PROC MEANS / SUMMARY / FORMAT.
  * Tier C (Agent Plan + Generate + Repair): RETAIN, FIRST. / LAST., MERGE ... IN=,
                            %MACRO, arrays, hash objects.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Tier(str, Enum):
    TIER_A = "Tier A (Deterministic AST · $0 Cost)"
    TIER_B = "Tier B (Hybrid AST + Meta LLM · $0.015)"
    TIER_C = "Tier C (Autonomous Agent Loop · $0.045)"


@dataclass
class RouteDecision:
    job_id: str
    dialect: str
    tier: Tier
    tier_code: str
    reasons: list[str]
    features: list[str] = field(default_factory=list)
    escalated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "dialect": self.dialect,
            "tier": self.tier.value,
            "tier_code": self.tier_code,
            "reasons": self.reasons,
            "features": self.features,
            "escalated": self.escalated,
        }


# Lexical patterns for T-SQL
RE_TSQL_TIER_C = [
    (re.compile(r"\bDECLARE\s+@\w+", re.I), "Variable declaration (DECLARE @)"),
    (re.compile(r"@\w+", re.I), "Variable usage (@var)"),
    (re.compile(r"\bWHILE\b", re.I), "Iterative WHILE loop"),
    (re.compile(r"\bCURSOR\b", re.I), "Procedural CURSOR"),
    (re.compile(r"\b(?:EXEC|EXECUTE)\b|\bsp_executesql\b", re.I), "Dynamic SQL execution (EXEC / sp_executesql)"),
    (re.compile(r"\bMERGE\s+INTO\b|\bMERGE\s+[a-zA-Z0-9_.\[\]]+\s+USING\b", re.I), "MERGE upsert statement"),
    (re.compile(r"##?[a-zA-Z0-9_]+", re.I), "Temporary table (#temp / ##temp)"),
    (re.compile(r"\bBEGIN\b[\s\S]*?\bEND\b", re.I), "Procedural block (BEGIN ... END)"),
]

RE_TSQL_TIER_B = [
    (re.compile(r"\bWITH\s+[a-zA-Z0-9_]+\s+AS\s*\(", re.I), "Common Table Expression (CTE)"),
    (re.compile(r"\bOVER\s*\(", re.I), "Window partition/ordering function (OVER)"),
    (re.compile(r"\b(?:PIVOT|UNPIVOT)\b", re.I), "PIVOT / UNPIVOT transformation"),
    (re.compile(r"\b(?:CROSS|OUTER)\s+APPLY\b", re.I), "APPLY operator (CROSS / OUTER APPLY)"),
]

# Lexical patterns for SAS
RE_SAS_TIER_C = [
    (re.compile(r"\bRETAIN\b", re.I), "RETAIN state accumulator"),
    (re.compile(r"\b(?:FIRST|LAST)\.[a-zA-Z0-9_]+", re.I), "BY-group boundary condition (FIRST. / LAST.)"),
    (re.compile(r"\bIN\s*=\s*[a-zA-Z0-9_]+", re.I), "Dataset merge tracking flag (IN=)"),
    (re.compile(r"%MACRO\b|%DO\b|%MEND\b", re.I), "SAS Macro processing (%MACRO / %DO)"),
    (re.compile(r"\bARRAY\b", re.I), "SAS Array declaration"),
    (re.compile(r"_TEMPORARY_\b", re.I), "Temporary SAS array"),
]

RE_SAS_TIER_B = [
    (re.compile(r"\bDATA\s+[a-zA-Z0-9_.]+", re.I), "SAS DATA step"),
    (re.compile(r"\bPROC\s+SORT\b", re.I), "PROC SORT with deduplication"),
    (re.compile(r"\bPROC\s+(?:MEANS|SUMMARY|FORMAT|FREQ|TRANSPOSE)\b", re.I), "Statistical / aggregate procedure"),
]


def route_job(source: str, dialect: str = "tsql", job_id: str = "job") -> RouteDecision:
    """Routes a legacy script to Tier A, B, or C with concrete reasoning."""
    clean = re.sub(r"/\*[\s\S]*?\*/", "", source)
    clean = re.sub(r"--.*$", "", clean, flags=re.M)
    d = dialect.lower().strip()

    if d == "sas":
        # 1. Tier C checks
        c_features = []
        for pat, desc in RE_SAS_TIER_C:
            if pat.search(clean):
                c_features.append(desc)
        if c_features:
            return RouteDecision(
                job_id=job_id,
                dialect="sas",
                tier=Tier.TIER_C,
                tier_code="C",
                reasons=["Complex procedural SAS detected. Requires Agent Plan call + Generate + 3-step Repair."],
                features=c_features,
            )

        # 2. Tier B checks
        b_features = []
        for pat, desc in RE_SAS_TIER_B:
            if pat.search(clean):
                b_features.append(desc)
        if b_features:
            return RouteDecision(
                job_id=job_id,
                dialect="sas",
                tier=Tier.TIER_B,
                tier_code="B",
                reasons=["Standard SAS DATA step or statistical PROC. Routed to Agent single-shot translation."],
                features=b_features,
            )

        # 3. Tier A checks (Pure PROC SQL)
        sql_only = bool(re.search(r"^\s*PROC\s+SQL\b[\s\S]*?\bQUIT\s*;\s*$", clean, re.I))
        disallowed = re.search(r"\b(calculated|monotonic|intnx|put\(|input\(|%)\b", clean, re.I)
        if sql_only and not disallowed:
            return RouteDecision(
                job_id=job_id,
                dialect="sas",
                tier=Tier.TIER_A,
                tier_code="A",
                reasons=["Pure PROC SQL statement without SAS procedural extensions. Transpiled deterministically."],
                features=["Pure PROC SQL"],
            )

        # Default SAS fallback
        return RouteDecision(
            job_id=job_id,
            dialect="sas",
            tier=Tier.TIER_B,
            tier_code="B",
            reasons=["SAS program with mixed steps. Routed to Tier B."],
            features=["SAS Generic"],
        )

    # T-SQL Routing
    # Lexical Pre-Scan
    c_features = []
    for pat, desc in RE_TSQL_TIER_C:
        if pat.search(clean):
            c_features.append(desc)

    if c_features:
        return RouteDecision(
            job_id=job_id,
            dialect="tsql",
            tier=Tier.TIER_C,
            tier_code="C",
            reasons=["Procedural or non-relational T-SQL construct detected. Agent execution loop required."],
            features=c_features,
        )

    b_features = []
    for pat, desc in RE_TSQL_TIER_B:
        if pat.search(clean):
            b_features.append(desc)

    # Subquery depth check
    subquery_depth = clean.count("SELECT") - 1
    if subquery_depth > 2:
        b_features.append(f"Deeply nested subqueries (depth {subquery_depth})")

    # AST validation via sqlglot if installed
    try:
        import sqlglot
        from sqlglot import exp
        parsed = sqlglot.parse(clean, read="tsql")
        for ast in parsed:
            if isinstance(ast, exp.Command):
                return RouteDecision(
                    job_id=job_id,
                    dialect="tsql",
                    tier=Tier.TIER_C,
                    tier_code="C",
                    reasons=["Command statement (exp.Command) cannot be transpiled via AST. Escalated to Tier C."],
                    features=["exp.Command"],
                )
    except ImportError:
        pass
    except Exception as e:
        # Real sqlglot parse syntax error escalates to Tier C
        return RouteDecision(
            job_id=job_id,
            dialect="tsql",
            tier=Tier.TIER_C,
            tier_code="C",
            reasons=[f"sqlglot parse error ({type(e).__name__}). Escalated to Tier C Autonomous Agent."],
            features=["sqlglot_parse_error"],
        )

    if b_features:
        return RouteDecision(
            job_id=job_id,
            dialect="tsql",
            tier=Tier.TIER_B,
            tier_code="B",
            reasons=["Analytical T-SQL constructs (CTE / Window / Pivot / Apply). Transpiled deterministically + verified."],
            features=b_features,
        )

    return RouteDecision(
        job_id=job_id,
        dialect="tsql",
        tier=Tier.TIER_A,
        tier_code="A",
        reasons=["Relational SELECT, JOIN, GROUP BY, WHERE. Handled 100% deterministically by sqlglot AST transpiler with $0 token cost."],
        features=["Relational AST"],
    )
