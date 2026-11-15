# EXLake.ai: Agentic Data Platform Modernisation
### Legacy T-SQL ETL & SAS Programs to Azure Lakehouse (PySpark + Delta Lake)

> **Repository:** [https://github.com/sLaYerUnleAsheD/birdie-pod](https://github.com/sLaYerUnleAsheD/birdie-pod)  
> **EXL Data & AI Hackathon — Challenge 03: Agentic Data Platform Modernisation**  
> Tracks: **GenAI & Agents** | **Data Engineering** | **Responsible AI**  
> *Bonus Deliverable Included:* EXLake.ai Evidence Layer (COBOL → Go byte-level verification engine).

---

## Executive Summary: The Problem

A large UK and Ireland insurer runs finance, actuarial, and claims reporting across **~1,200 legacy on-premise T-SQL ETL jobs** and **400+ SAS programs**. 
- Business logic is undocumented.
- End-to-end data lineage is unknown.
- Upstream migrations fail because manual code translations hallucinate edge cases, date math, and joins.
- **Every migrated cloud pipeline must prove exact output reconciliation** with historical legacy baseline extracts before cutover.

**EXLake.ai** is an AI-native agentic migration factory that deterministically parses, translates, and mathematically reconciles legacy T-SQL and SAS jobs to cloud-native **PySpark on an Azure Lakehouse (Medallion Delta architecture)**.

---

## 3-Tier Anti-Hallucination Routing Architecture (`dpm/router.py`)

Relying entirely on LLMs to generate SQL translations produces silent hallucinations (scope leaks, implicit datatype conversions, and phantom joins). EXLake.ai enforces a strict **3-Tier Routing Triage** using AST analysis and robust lexical pre-scanning before any code generation:

```
                            [ Legacy Script (.sql / .sas) ]
                                          │
                                          ▼
                      ┌───────────────────────────────────────┐
                      │             dpm/router.py             │
                      │      3-Tier Deterministic Triage      │
                      └───────────────────┬───────────────────┘
                                          │
         ┌────────────────────────────────┼────────────────────────────────┐
         ▼                                ▼                                ▼
  [ Tier A: Easy ]                 [ Tier B: Medium ]               [ Tier C: Hard ]
  • T-SQL: SELECT / JOIN / GROUP   • T-SQL: CTEs, OVER(), PIVOT     • T-SQL: MERGE, #temp, DECLARE
  • SAS: Pure PROC SQL only        • SAS: DATA + PROC SORT/MEANS    • SAS: RETAIN, FIRST./LAST., %MACRO
         │                                │                                │
         ▼                                ▼                                ▼
   sqlglot AST Engine       AST + Meta LLM / Claude skill     Meta LLM / Claude Skill Agent
($0 Token Cost, Deterministic)  (Lineage Annotations + PySpark)  (Self-Healing Reconciliation Loop)
```

| Tier | T-SQL Characteristics | SAS Characteristics | Engine & Tooling | Token Cost | Parity Gate |
|---|---|---|---|---|---|
| **Tier A (Easy)** | Standard relational `SELECT`, `INNER/LEFT JOIN`, `WHERE`, `GROUP BY`, `#temp` staging | Pure `PROC SQL; ... QUIT;` (No `DATA` steps, no `%MACRO`) | `sqlglot` AST transpiler (`tsql` $\to$ `spark`) | **$0.000** | 100% Deterministic |
| **Tier B (Medium)** | CTEs (`WITH ... AS`), Windowing (`OVER(...)`), `PIVOT/UNPIVOT`, `APPLY` | `DATA` steps, `PROC SORT NODUPKEY`, `PROC MEANS/SUMMARY`, `PROC FORMAT` | AST extracts skeleton + Meta LLM / Claude skill wraps DataFrames | **~$0.015** | Parallel-run diff |
| **Tier C (Hard)** | Procedural scripts (`DECLARE @var`, `WHILE`, `CURSOR`, `MERGE`, dynamic SQL) | Stateful syntax (`RETAIN`, `FIRST.`, `LAST.`, `%MACRO` loops, `MERGE IN=`) | Multi-step Meta LLM / Claude Skill Agent: Plan $\to$ PySpark $\to$ Self-Heal | **~$0.045** | Self-healing parity |

---

## 12 Canonical Benchmark Jobs & Traps (`dpm/jobs/`)

The repository includes 12 canonical legacy benchmark jobs (8 T-SQL, 4 SAS) in [`dpm/jobs/`](dpm/jobs/), each designed with a specific trap that reconciliation must detect:

| Job ID | Name | Dialect | Tier | Benchmark Trap Addressed | Oracle Ground Truth |
|:---:|---|:---:|:---:|---|:---:|
| `tsql_01` | Premium by Product | T-SQL | **Tier A** | Decimal precision of `SUM` (`ROUND_HALF_UP`) | `sqlserver` |
| `tsql_02` | Claims by Region & Status | T-SQL | **Tier A** | Case-insensitive `GROUP BY` ('open' vs 'OPEN') via `F.lower()` | `sqlserver` |
| `tsql_03` | Top-N Open Claims | T-SQL | **Tier A** | `TOP` + `ORDER BY` ties and native `ISNULL` to `coalesce` | `sqlserver` |
| `tsql_04` | Running Reserve Window | T-SQL | **Tier B** | Window frame default (`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`) | `sqlserver` |
| `tsql_05` | Latest Claim per Policy | T-SQL | **Tier B** | `ROW_NUMBER` tie-break in CTE pipeline | `sqlserver` |
| `tsql_06` | Monthly Loss Ratio | T-SQL | **Tier B** | Integer floor division (`div` vs `/`) and `EOMONTH`/`DATEADD` | `sqlserver` |
| `tsql_07` | Reserve Waterfall | T-SQL | **Tier C** | Procedural `WHILE` loop + `#temp` table simulation | `sqlserver` |
| `tsql_08` | Policy Snapshot Upsert | T-SQL | **Tier C** | `MERGE` statement upsert semantics | `sqlserver` |
| `sas_01` | Paid by Line of Business | SAS | **Tier A** | Pure `PROC SQL` transpilation without procedural constructs | `sas-snapshot` |
| `sas_02` | Loss Deduplication & Summary | SAS | **Tier B** | Missing numeric ordering (`.` sorts lowest) + `PROC SORT NODUPKEY` | `sas-snapshot` |
| `sas_03` | Multi-Source Merge with IN= | SAS | **Tier C** | `MERGE` with `IN=` tracking flags | `sas-snapshot` |
| `sas_04` | Loss Development Accumulator | SAS | **Tier C** | `RETAIN` accumulator + `FIRST.`/`LAST.` BY-group resets | `sas-snapshot` |

---

## Autonomous Repair Loop: EXLake.ai Meta LLM / Claude Skill (`dpm/agents.py`, `skills/modernize/SKILL.md`)

When the deterministic AST transpiler or reconciliation sandbox detects an execution error or data mismatch, the orchestrator triggers the **EXLake.ai Meta LLM / Claude Repair Agent**. Operated programmatically via Python or through the Claude Code `/modernize` slash skill (`skills/modernize/SKILL.md`), the agent executes autonomous self-healing. 

The agent operates under a strict surgical protocol to fix semantic drops rather than rewriting architecture from scratch.

### Input Schema (3-Artifact Bundle)
For every repair attempt, the orchestrator packages three exact artifacts:
1. **Source Legacy Code**: The original T-SQL or SAS script.
2. **Failed PySpark Attempt**: The latest candidate script from the transpiler or previous attempt.
3. **Sandbox Error Report**: Either a Python compilation stack trace OR the mathematical reconciliation diff report showing exactly where row counts, schemas, or values mismatched.

### Phase 5 Semantic Guardrails
- **Filter Preservation**: Every legacy `WHERE` and `HAVING` clause is strictly mapped to `.filter()` on the DataFrame.
- **Join Fidelity**: Join types are strictly conserved. If legacy specifies `LEFT JOIN`, PySpark must enforce `how="left"` (never defaulting to inner joins).
- **Complex Math Translation**: Before applying mathematical operators (`+`, `-`, `*`, `/`) between columns, every individual column is wrapped in `coalesce` to eliminate null propagation:
  - *Legacy*: `ISNULL(A, 0) + ISNULL(B, 0)`
  - *Correct PySpark*: `F.coalesce(F.col("A"), F.lit(0)) + F.coalesce(F.col("B"), F.lit(0))`
  - *Fatal Error*: `F.coalesce(F.col("A") + F.col("B"), F.lit(0))` (nulls out the entire sum if either operand is null).

### Phase 4 Enterprise Syntactic Standards
- **Medallion Layer Separation**: Preserves distinct stages for Bronze (Ingestion), Silver (Conformed Joins & Filters), and Gold (Aggregations & Projections).
- **Explicit Aliasing**: All DataFrames are immediately aliased (e.g. `.alias("c")`).
- **Explicit Join Equality**: Join conditions use strict column references (e.g., `F.col("c.ID") == F.col("p.ID")`).
- **Aggregation Structure**: Dimension columns and aggregate metrics are never mixed in `.select()`. All aggregations strictly use `.groupBy(...).agg(...)`.

### LLM Provider Configuration (`core/llm.py`)
Configured to seamlessly access Meta and cloud inference endpoints via standard environment keys:
- `META_API_URL` & `META_API_KEY`: Direct Meta model endpoint access (`muse-spark-1.3`).
- `MODAL_API_URL`, `MODAL_API_KEY`, & `MODAL_MODEL`: Modal shared endpoints.
- `BASETEN_API_KEY` & `BASETEN_MODEL`: Baseten Model APIs.
- Built-in disk cache, per-provider spend caps (`MAX_SPEND_META_USD`), and JSONL audit ledger.

---

## Ground-Truth Oracle Engine & Exact Reconciliation (`dpm/reconcile.py`)

Code equivalence is **never** validated by inspecting text or using sloppy `np.isclose`. EXLake.ai enforces exact mathematical verification:

1. **Declared Oracle Engine**:
   - **T-SQL**: SQL Server 2022 container (`mcr.microsoft.com/mssql/server`, `oracle_kind: sqlserver`).
   - **SAS**: Independent frozen reference snapshot (`oracle_kind: sas-snapshot`).
   - Every scorecard explicitly displays its `oracle_kind`.
2. **Exact Decimal Equality**:
   - Decimal columns: Exact `Decimal` equality after quantizing to declared scale (`ROUND_HALF_UP`).
   - Control totals: Summed as exact `Decimal` (zero floating-point accumulation drift).
   - Tolerance (rel $10^{-9}$) only allowed for declared `FLOAT` columns.
3. **Primary Key Outer Merge Diff**:
   - Full outer merge identifies exact counts of missing rows, extra rows, and changed cells.
   - Returns the first 10 concrete discrepancy examples as `(key, column, expected, actual)`.
4. **Multiset Row Hash Fallback**:
   - When no primary key is declared, datasets are compared as multisets of row hash signatures.
5. **Self-Healing Diagnostic Feedback**:
   - `scorecard.as_repair_text(max_chars=2500)` feeds structured error diffs directly into the agent repair loop.

---

## Dialect Divergence Post-Rules (`dpm/transpile.py`)

When transpiling T-SQL to PySpark, deterministic post-rules eliminate common SQL Server vs Spark semantic gaps:
- **Case-Insensitive String Comparison**: Automatically injects `F.lower(col) == F.lower(lit)` to mirror SQL Server default CI collation.
- **Null-Safe String Concatenation**: Translates `ISNULL(a, b)` to `F.coalesce(F.col("a"), F.lit(b))` and ensures SQL Server null-propagation semantics.
- **Integer Division**: Casts integer division `(a / b).cast("bigint")` to prevent PySpark float-division drift.
- **Date Boundary Counting**: Accurate translation of `DATEDIFF(day, ...)` to `F.datediff` and `DATEDIFF(month, ...)` to `F.floor(F.months_between(...))`.
- **Explicit Schema Enforcement**: Generated scripts define `run(spark, in_dir, out_dir)` reading CSVs with explicit `StructType` schemas (never `inferSchema`).

---

## Medallion Architecture, DAG & Approval Gate (`dpm/operate.py`)

- **Medallion Layers**:
  - **Bronze**: Raw ingestions (`STG_*`, `RAW_*`).
  - **Silver**: Conformed intermediate staging and joined tables.
  - **Gold**: Terminal reporting marts and actuarial loss development models.
- **Workflow DAG (`viewer/data/dpm/workflow.json`)**:
  - Automatically constructed from topological wave analysis (`dpm/discover.py`).
  - Contains tasks, `depends_on`, cron schedules (`0 2 * * *`), and SLA minutes.
- **OpenLineage Standard**:
  - Emits OpenLineage run events with dataset facets and oracle lineage tracking.
- **Human Approval Gate**:
  - Reads `approvals.json` (defaults to `false`).
  - `operate.py` strictly blocks cutover or publishing of any unapproved job until a human operator signs off.

---

## Dedicated Challenge 03 Modernization Studio (UI)

Open [`viewer/index.html`](viewer/index.html) in any modern browser (zero npm dependencies, zero build step):
- **Default Route (`#/modernize`)**: Directly surfaces Challenge 03 as the hero deliverable.
- **Directory & Upload Tree**: Retains all 12 canonical benchmark jobs + user-uploaded scripts (e.g., `sample.tsql`) with exact original filenames.
- **Live 3-Tier Routing**: Real-time syntax inspection showing AST rules, detected features, and token cost breakdown.
- **Medallion & Data Contract Visualizer**: Interactive Bronze $\to$ Silver $\to$ Gold stage transition with data contract assertion checks.
- **Side-by-Side Code Workspace**: Legacy T-SQL/SAS displayed side-by-side with generated PySpark code with one-click copy.
- **Column-Level Lineage Tracing**: Complete table showing source expressions, target lakehouse columns, and transformation types.
- **Reconciliation Dashboard**: Visual 4-check scorecard proving exact record-for-record parity against the declared oracle.

---

## Quickstart & Verification Commands

### 1. Run Complete Test Suite (30 Unit Tests, Zero Warnings)
```bash
python -m unittest tests/test_lakehouse.py tests/test_dpm_router.py tests/test_dpm_transpile.py tests/test_dpm_reconcile.py tests/test_dpm_pipeline.py tests/test_dpm_operate.py
```
*Output: `Ran 30 tests in 0.885s — OK (ALL PASSING)`*

### 2. Run the Benchmark Runner
```bash
python -m dpm.benchmark [--cost-cap 5.0] [--force-agent]
```

### 3. Export Studio Assets
```bash
python -m dpm.export
```

### 4. Launch the Modernization Viewer
```bash
python serve.py
```
Navigate to: `http://localhost:8000/#/modernize`

---

## Bonus Deliverable: EXLake.ai COBOL → Go Verification Engine

In addition to Challenge 03, this repository retains the **EXLake.ai COBOL Engine** as a bonus capability:
- Compiler oracle execution via GnuCOBOL.
- Isolated Docker sandbox (`core/sandbox.py`) for safe model code execution.
- Automated repair loops diffing stdout byte-for-byte on synthetic fixed-width inputs.
- Inspectable under the **Explorer (COBOL)** tab in `viewer/index.html`.
