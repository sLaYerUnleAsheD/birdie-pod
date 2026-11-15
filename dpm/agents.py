"""Autonomous Meta-Spark Repair Agent Integration for Tier C Self-Healing Loops.

Implements:
1. Exact System Prompt: Aegis Meta-Spark Repair Agent.
2. 3-Artifact Input Schema:
   - Source Legacy Code (T-SQL or SAS)
   - Failed PySpark Attempt (AST output)
   - Sandbox Error Report (Stack trace or DataComPy / Reconcile diff report)
3. Phase 5 Guardrails (Filter preservation, join fidelity, individual coalesce math).
4. Phase 4 Rules (Medallion layers, explicit aliasing, explicit join keys, .groupBy().agg()).
5. Native integration with core.llm (META_API_KEY, MODAL_API_KEY, BASETEN_API_KEY).
"""
from __future__ import annotations
import json
import os
import re
from pathlib import Path
from typing import Any

from core.llm import LLMClient, LLMResult, load_env


META_SPARK_REPAIR_SYSTEM_PROMPT = """System Prompt: Aegis Meta-Spark Repair Agent

Role & Objective
You are the Meta-Spark Repair Agent, a Senior Lakehouse Migration Architect operating within the Aegis Data Platform. Your purpose is to repair PySpark migration scripts that failed our deterministic AST parser's verification gate. You do not write code from scratch; you surgically fix the dropped logic, syntax failures, or reconciliation mismatches to achieve 100% data parity with the legacy T-SQL/SAS output.

Input Schema
You will receive three artifacts for every task:

Source Legacy Code: The original T-SQL or SAS script.

Failed PySpark Attempt: The output from the deterministic AST transpiler.

Sandbox Error Report: Either a Python compilation stack trace OR a DataComPy diff report showing exactly where the row counts, schemas, or data values mismatched.

Execution Protocol

Step 1: Root Cause Analysis
Analyze the Sandbox Error Report against the Source Legacy Code. Identify exactly which legacy clauses were dropped or mistranslated by the AST transpiler. Look specifically for missing WHERE clauses, flipped JOIN types, or failed mathematical operations on nulls.

Step 2: Strict Clause-for-Clause Parity (Phase 5 Guardrails)
You must reconstruct the PySpark code to ensure zero business logic is lost.

Filter Preservation: Every WHERE and HAVING clause in the legacy code must be mapped to a .filter() on the PySpark DataFrame.

Join Fidelity: You must strictly preserve the join types. If the legacy code says LEFT JOIN, your PySpark must say how="left". Do not default to inner joins.

Complex Math Translation: Before applying any mathematical operator (+, -, *, /) between columns, you must wrap each individual column reference in a coalesce.

Legacy: ISNULL(A, 0) + ISNULL(B, 0)

Correct PySpark: F.coalesce(F.col("A"), F.lit(0)) + F.coalesce(F.col("B"), F.lit(0))

Fatal Error: F.coalesce(F.col("A") + F.col("B"), F.lit(0)) (This will yield null if either is null).

Step 3: Enforce PySpark Syntactic Guardrails (Phase 4 Rules)
The repaired code must still adhere to our enterprise architectural standards:

Medallion Architecture: Maintain the separation of Bronze (Ingestion), Silver (Joins/Filters), and Gold (Aggregations/Projections).

Explicit Aliasing: Ensure all ingested DataFrames are aliased immediately (e.g., .alias("c")).

Explicit Join Keys: Use exact column equality for joins (e.g., F.col("c.ID") == F.col("p.ID")). Do not use string placeholders.

Aggregation Structure: Never mix dimension columns and aggregate functions inside a .select(). All aggregations must use .groupBy(...).agg(...).

Output Format
Output ONLY the fully repaired, execution-ready PySpark code. Do not include markdown conversational filler, apologies, or explanations. Begin the file with the standard Aegis header, updating the Routing tier to Tier C (Agentic Repair)."""


def format_repair_agent_input(
    source_legacy_code: str,
    failed_pyspark_attempt: str,
    sandbox_error_report: str,
    job_id: str = "job",
) -> str:
    """Bundles the three exact artifacts required by the Aegis Meta-Spark Repair Agent."""
    return f"""### ARTIFACT 1: Source Legacy Code ({job_id})
```sql
{source_legacy_code.strip()}
```

### ARTIFACT 2: Failed PySpark Attempt
```python
{failed_pyspark_attempt.strip()}
```

### ARTIFACT 3: Sandbox Error Report
```
{sandbox_error_report.strip()}
```

Follow the Execution Protocol (Root Cause Analysis, Phase 5 Clause Parity, Phase 4 Syntactic Guardrails) and output ONLY the repaired, execution-ready PySpark code."""


def get_llm_client(provider: str | None = None) -> LLMClient:
    """Initializes LLMClient using environment configuration from .env / env.example."""
    load_env()
    if not provider:
        if os.environ.get("META_API_KEY"):
            provider = "meta"
        elif os.environ.get("MODAL_API_KEY"):
            provider = "modal"
        elif os.environ.get("BASETEN_API_KEY"):
            provider = "baseten"
        else:
            provider = "meta"

    return LLMClient(provider=provider)


def clean_agent_code(raw_response: str) -> str:
    """Extracts raw, executable PySpark code, stripping any conversational markdown fences if present."""
    text = raw_response.strip()
    match = re.search(r"```(?:python|pyspark)?\s*([\s\S]*?)```", text)
    if match:
        code = match.group(1).strip()
    else:
        code = text

    # Ensure header designates Tier C (Agentic Repair)
    if "# Routing: Tier C" not in code and "# Tier: Tier C" not in code:
        header = """# =====================================================================
# Aegis Modernize: Automated Lakehouse Transpiler
# Tier: Tier C (Agentic Repair · Meta-Spark LLM Verification Loop)
# Architecture: Medallion Architecture (Bronze -> Silver -> Gold Mart)
# Phase 4/5 Rules: Coalesced Math, Explicit Aliasing, Exact Join Keys, .groupBy().agg()
# =====================================================================\n\n"""
        if code.startswith("# ====================================================================="):
            code = re.sub(
                r"# Tier:[^\n]*\n",
                "# Tier: Tier C (Agentic Repair · Meta-Spark LLM Verification Loop)\n",
                code
            )
        else:
            code = header + code

    return code


def repair_pyspark_with_meta_agent(
    source_legacy_code: str,
    failed_pyspark_attempt: str,
    sandbox_error_report: str,
    client: LLMClient | None = None,
    job_id: str = "job",
    provider: str | None = None,
) -> tuple[str, LLMResult]:
    """Invokes the Meta-Spark Repair Agent to surgically repair failed PySpark code."""
    if client is None:
        client = get_llm_client(provider=provider)

    user_prompt = format_repair_agent_input(
        source_legacy_code=source_legacy_code,
        failed_pyspark_attempt=failed_pyspark_attempt,
        sandbox_error_report=sandbox_error_report,
        job_id=job_id,
    )

    result = client.call(
        prompt=user_prompt,
        system=META_SPARK_REPAIR_SYSTEM_PROMPT,
        tag=f"repair_{job_id}",
    )

    repaired_code = clean_agent_code(result.text)
    return repaired_code, result


# -----------------------------------------------------------------------------
# Backwards Compatibility Helpers for Planning & Legacy Prompts
# -----------------------------------------------------------------------------

format_repair_prompt = format_repair_agent_input


def format_translate_prompt(job_id: str, dialect: str, source_code: str, meta: dict[str, Any]) -> str:
    return f"""### AEGIS DATA PLATFORM MODERNIZATION RULES
Target: {job_id} ({dialect.upper()})
Metadata: {json.dumps(meta, indent=2)}
Source:
```{dialect}
{source_code}
```"""


def format_plan_prompt(job_id: str, source_code: str, meta: dict[str, Any]) -> str:
    return f"""### TIER C ARCHITECTURAL PLAN: {job_id}
Source:
```{meta.get('dialect', 'sql')}
{source_code}
```"""


def parse_agent_response(text: str) -> tuple[str, dict[str, Any]]:
    py_code = clean_agent_code(text)
    json_match = re.search(r"```json\s*([\s\S]*?)```", text)
    lineage = {}
    if json_match:
        try:
            lineage = json.loads(json_match.group(1))
        except Exception:
            pass
    return py_code, lineage
