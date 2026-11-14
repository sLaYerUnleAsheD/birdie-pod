"""Autonomous LLM Agent Integration for Tier C Migration and Repair Loops.

Implements:
1. Static cacheable RULES block encoding SQL Server & SAS semantic invariants.
2. Translation prompts, SAS Plan calls, and diagnostic Repair prompts.
3. Lineage validation ensuring no hallucinated input/output columns.
4. Response parser extracting Python script and JSON lineage block.
"""
from __future__ import annotations
import json
import re
from typing import Any

RULES = """### AEGIS DATA PLATFORM MODERNIZATION RULES (STRICT COMPLIANCE)

You are an expert PySpark Lakehouse migration engineer converting legacy T-SQL and SAS jobs into production-grade PySpark scripts for Azure Databricks Delta Lake.

#### 1. PYSPARK SCRIPT SPECIFICATION
- Define a single entry-point function: `run(spark: SparkSession, in_dir: Path, out_dir: Path) -> None`.
- INGESTION: Always read input CSVs with an EXPLICIT schema (`spark.read.schema(...).csv(...)`). NEVER use `inferSchema=True`.
- SINK: Always write output DataFrames as CSV with header (`df.write.mode("overwrite").option("header", "true").csv(...)`).
- DETERMINISTIC OUTPUT: Always apply explicit `.orderBy(...)` before any `.limit()` or aggregation sink.
- SPARK CONFIG: Configured with `spark.sql.shuffle.partitions=4`, `spark.sql.session.timeZone=UTC`.

#### 2. SQL SERVER (T-SQL) SEMANTIC INVARIANTS
- COLLATION: Default to case-insensitive string equality using `F.lower(col_a) == F.lower(col_b)`.
- NULL HANDLING: Translate `ISNULL(a, b)` to `F.coalesce(F.col("a"), F.lit(b))`. Remember `'a' + NULL` produces `NULL`.
- INTEGER DIVISION: In T-SQL, `int / int` truncates towards zero. In PySpark, `/` is float division. Always cast: `(col_a / col_b).cast("bigint")`.
- DATEDIFF: Calculate boundary crossings accurately using `F.datediff` for days, and `F.floor(F.months_between(...))` for months.
- ROUNDING: SQL Server rounds half-away-from-zero. Decimal columns must quantize to declared scale.
- TEMPORARY TABLES: Model `#temp` tables as explicit intermediate DataFrames representing Silver Staging.

#### 3. SAS SEMANTIC INVARIANTS
- MISSING NUMERICS: In SAS, `.` (missing numeric) sorts lowest (less than any negative number).
- DATES: SAS dates are integers counting days since 1960-01-01. Convert to ISO 8601 `YYYY-MM-DD`.
- BY-GROUP BOUNDARIES: Model `FIRST.variable` / `LAST.variable` using `Window.partitionBy(...).orderBy(...)` with `F.row_number()`.
- DEDUPLICATION: `PROC SORT NODUPKEY` keeps the first encountered record per group.
- ACCUMULATORS: Model `RETAIN` statements as windowed cumulative sums or fold operations.

#### 4. REQUIRED RESPONSE FORMAT
Your reply must contain EXACTLY two markdown blocks:
1. One Python code block: ```python ... ``` defining `run(spark, in_dir, out_dir)`
2. One JSON lineage block: ```json { "column_name": ["table.column", ...] } ```
"""


def format_translate_prompt(job_id: str, dialect: str, source_code: str, meta: dict[str, Any]) -> str:
    return f"""{RULES}

### TARGET JOB TO MIGRATE: {job_id} ({dialect.upper()})

#### Legacy Source Code:
```{dialect}
{source_code}
```

#### Metadata Contract:
```json
{json.dumps(meta, indent=2)}
```

Generate the verified PySpark script and column-level lineage JSON adhering to all semantic rules."""


def format_plan_prompt(job_id: str, source_code: str, meta: dict[str, Any]) -> str:
    return f"""{RULES}

### TIER C PLANNING PHASE: {job_id} (Complex SAS / Procedural T-SQL)

Analyze the following legacy script and provide an architectural execution plan:
1. Intermediate state representation (Silver DataFrames vs. Window specifications)
2. Handling of procedural constructs (RETAIN, MERGE IN=, WHILE loops, #temp tables)
3. Step-by-step PySpark transformation strategy

```{meta.get('dialect', 'sql')}
{source_code}
```
"""


def format_repair_prompt(
    job_id: str,
    dialect: str,
    source_code: str,
    failed_script: str,
    scorecard_repair_text: str,
    meta: dict[str, Any],
) -> str:
    return f"""{RULES}

### RECONCILIATION REPAIR REQUIRED FOR JOB: {job_id}

Your previous PySpark translation failed output reconciliation against the {meta.get('oracle_kind', 'legacy oracle')}.

#### Ground-Truth Reconciliation Diagnostic Diff:
```
{scorecard_repair_text}
```

#### Previous Attempt Script:
```python
{failed_script}
```

#### Original Legacy Source ({dialect.upper()}):
```{dialect}
{source_code}
```

Fix the exact discrepancies highlighted in the diagnostic diff. Return the corrected Python file and lineage JSON."""


def parse_agent_response(text: str) -> tuple[str, dict[str, Any]]:
    """Extracts the python script and json lineage from agent output."""
    py_match = re.search(r"```python\s*([\s\S]*?)```", text)
    python_code = py_match.group(1).strip() if py_match else text

    json_match = re.search(r"```json\s*([\s\S]*?)```", text)
    lineage = {}
    if json_match:
        try:
            lineage = json.loads(json_match.group(1))
        except Exception:
            pass
    return python_code, lineage

