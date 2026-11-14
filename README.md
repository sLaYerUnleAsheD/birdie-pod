# Aegis Modernize: Agentic Data Platform Modernisation
### Legacy T-SQL ETL & SAS Programs to Azure Lakehouse (PySpark + Delta Lake)

> **EXL Data & AI Hackathon — Challenge 03: Agentic Data Platform Modernisation**  
> Tracks: **GenAI & Agents** | **Data Engineering** | **Responsible AI**  
> *Bonus Deliverable Included:* Aegis Evidence Layer (COBOL → Go byte-level verification engine).

---

## Executive Summary: The Problem

A large UK and Ireland insurer runs finance, actuarial, and claims reporting across **~1,200 legacy on-premise T-SQL ETL jobs** and **400+ SAS programs**. 
- Business logic is undocumented.
- End-to-end data lineage is unknown.
- Upstream migrations fail because manual code translations hallucinate edge cases, date math, and joins.
- **Every migrated cloud pipeline must prove exact output reconciliation** with historical legacy baseline extracts before cutover.

**Aegis Modernize** is an AI-native agentic migration factory that deterministically parses, translates, and mathematically reconciles legacy T-SQL and SAS jobs to cloud-native **PySpark on an Azure Lakehouse (Medallion Delta architecture)**.

---

## 3-Tier Anti-Hallucination Routing Architecture

Relying entirely on LLMs to generate SQL translations produces silent hallucinations (scope leaks, implicit datatype conversions, and phantom joins). Aegis enforces a strict **3-Tier Routing Triage** before any code generation:

```
                            [ Legacy Script (.sql / .sas) ]
                                          │
                                          ▼
                      ┌───────────────────────────────────────┐
                      │      lakehouse_adapter/router.py      │
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
   sqlglot AST Engine             AST Skeleton + Meta LLM           Autonomous Meta Agent
($0 Token Cost, Deterministic)  (Lineage Annotations + PySpark)  (Self-Healing Reconciliation Loop)
```

| Tier | T-SQL Characteristics | SAS Characteristics | Engine & Tooling | Token Cost | Parity Gate |
|---|---|---|---|---|---|
| **Tier A (Easy)** | Standard relational `SELECT`, `INNER/LEFT JOIN`, `WHERE`, `GROUP BY` | Pure `PROC SQL; ... QUIT;` (No `DATA` steps, no `%MACRO`) | `sqlglot` AST transpiler (`tsql` $\to$ `spark`) | **$0.000** | 100% Deterministic |
| **Tier B (Medium)** | CTEs (`WITH ... AS`), Windowing (`OVER(PARTITION BY...)`), `PIVOT` | `DATA` steps, `PROC SORT`, `PROC MEANS`, `PROC SUMMARY`, `PROC FORMAT` | AST extracts skeleton + Meta model wraps modular PySpark DataFrames | **~$0.015** | Parallel-run diff |
| **Tier C (Hard)** | Procedural scripts (`DECLARE @var`, `WHILE`, `CURSOR`, `#temp_tables`, `MERGE`) | Stateful syntax (`RETAIN`, `FIRST.`, `LAST.`, `%MACRO` loops, `MERGE IN=`) | Multi-step Meta Agent: Plan $\to$ Generate PySpark $\to$ Run $\to$ Self-Heal | **~$0.045** | Self-healing parity |

---

## Medallion Lakehouse Architecture & Data Contracts

Every migrated pipeline adheres to the **Azure Lakehouse Medallion Architecture**:
- **Bronze**: Raw landing ingestion from on-premise sources (`STG_*`, `RAW_*`).
- **Silver**: Conformed, cleaned, deduplicated, and FX-adjusted entities with schema contracts (`FCT_*`, `DIM_*`).
- **Gold**: Aggregated business reporting marts, actuarial loss development triangles, and Solvency II capital models.

### Data Contract Enforcement (`lakehouse_adapter/contracts.py`)
Pipelines enforce schema definitions, non-null assertions, primary key uniqueness, and value domains before publishing:
```python
CONTRACT_CLAIMS_SILVER = DataContract(
    contract_id="CONTRACT_CLM_SILVER_001",
    layer=MedallionLayer.SILVER,
    table_name="silver_curated_claims",
    primary_keys=["claim_id"],
    columns=[
        ColumnContract(name="claim_id", data_type="STRING", nullable=False),
        ColumnContract(name="net_paid_amount", data_type="DECIMAL", min_value=0.0),
        ColumnContract(name="claim_status", allowed_values=["OPEN", "CLOSED", "PENDING", "SETTLED"]),
    ]
)
```

---

## 4-Point Production Parity Reconciliation (`reconcile.py`)

Code equivalence is **never** validated by inspecting text. Aegis runs both programs on identical inputs and executes a mathematical 4-point reconciliation:

```
[ Legacy Output CSV ] ──┐
                         ├─► [ lakehouse_adapter/reconcile.py ] ──► Scorecard (100% Parity)
[ PySpark Output CSV ] ─┘
```

1. **Check 1 — Row Count Parity**:
   $$\text{len}(DF_{\text{legacy}}) == \text{len}(DF_{\text{spark}}) \quad (\Delta_{\text{rows}} = 0)$$
2. **Check 2 — Schema & Column Names**:
   $$\text{set}(DF_{\text{legacy}}.\text{columns}) == \text{set}(DF_{\text{spark}}.\text{columns})$$
3. **Check 3 — Aggregate Control Totals**:
   $$\left|\sum V_{\text{legacy}} - \sum V_{\text{spark}}\right| \le 10^{-5} \times \left|\sum V_{\text{legacy}}\right|$$
   Verifies financial balances (e.g., `paid_amount`, `total_open_reserves`, `earned_premium`).
4. **Check 4 — Row-by-Row Cell Diff**:
   Sorts both datasets by primary keys, resets indices, and executes:
   ```python
   pd.testing.assert_frame_equal(sorted_legacy, sorted_spark, check_dtype=False, atol=1e-4)
   ```

---

## Real Insurance Benchmark Datasets & End-to-End Execution

No dummy mockups: all 6 benchmark jobs in `benchmark/legacy_jobs/` run end-to-end against real, multi-record relational datasets:
- **`benchmark/data/policies.csv`**: 50 policies across Dublin, Leinster, Munster, Connacht.
- **`benchmark/data/claims.csv`**: 75 claims with loss dates, report dates, reserves, and cause codes.
- **`benchmark/data/payments.csv`**: 120 claim payment transactions (Indemnity, Salvage, Expense).

### Verified Benchmark Suite Results

```bash
python scripts/test_legacy_jobs_benchmark.py
```

| Job Name | Type | Tier | Input Tables | Output Rows | Lineage Columns | Parity Result |
|---|---|---|---|---|---|---|
| `job_01_regional_loss_summary.sql` | T-SQL | **Tier A** | `policies`, `claims`, `payments` | 4 summary rows | 6 columns | **100.0% PASS** |
| `job_02_reporting_lag.sql` | T-SQL | **Tier A** | `claims`, `policies` | 75 claims | 7 columns | **100.0% PASS** |
| `job_03_cumulative_exposure.sql` | T-SQL | **Tier B** | `claims`, `payments` | 50 claims (Top 50) | 6 columns | **100.0% PASS** |
| `job_04_actuarial_severity_bands.sas` | SAS | **Tier B** | `raw.claims` | 10 band cells | 6 columns | **100.0% PASS** |
| `job_05_payment_trajectory.sas` | SAS | **Tier C** | `raw.payments` | 75 rolled-up claims | 6 columns | **100.0% PASS** |
| `job_06_macro_leakage_audit.sas` | SAS | **Tier C** | `policies`, `claims`, `payments` | 4 audit anomalies | 6 columns | **100.0% PASS** |

*All pre-computed production snapshots are persisted in `benchmark/legacy_jobs/expected_outputs/`.*

---

## Dedicated Challenge 03 Modernization Studio (UI)

Open [`viewer/index.html`](viewer/index.html) in any modern browser (zero npm dependencies, zero build step):
- **Default Route (`#/modernize`)**: Directly surfaces Challenge 03 as the hero deliverable.
- **Pipeline Selector & Upload**: Choose between 18 benchmark insurance pipelines or upload any custom `.sql` / `.sas` file.
- **Live 3-Tier Routing Card**: Real-time syntax inspection showing AST rules, detected features, and token cost breakdown.
- **Medallion & Data Contract Visualizer**: Interactive Bronze $\to$ Silver $\to$ Gold stage transition with data contract assertion checks.
- **Side-by-Side Code Workspace**: Legacy T-SQL/SAS displayed side-by-side with generated PySpark code with one-click copy.
- **Column-Level Lineage Tracing**: Complete table showing source expressions, target lakehouse columns, and transformation types.
- **Reconciliation Dashboard**: Visual 4-check scorecard proving exact record-for-record parity.

---

## Quickstart & Verification Commands

### 1. Run Unit & Integration Tests (Zero Dependencies or with Pandas)
```bash
python -m unittest tests/test_lakehouse.py
```

### 2. Run End-to-End Execution on Real Insurance Datasets
```bash
python lakehouse_adapter/engine.py
```

### 3. Run the Reconciliation Validator CLI
```bash
python reconcile.py --legacy benchmark/legacy_jobs/expected_outputs/job_01_regional_loss_summary_legacy.csv --spark benchmark/legacy_jobs/expected_outputs/job_01_regional_loss_summary_spark.csv --pk region product_line
```

### 4. Launch the Modernization Viewer
Simply open `viewer/index.html` in your browser:
```bash
# Optional local HTTP server (or double-click viewer/index.html)
python -m http.server 8000 --directory viewer
```
Navigate to: `http://localhost:8000/#/modernize`

---

## Bonus Deliverable: Aegis COBOL → Go Verification Engine

In addition to Challenge 03, this repository retains the **Aegis Migrate COBOL Engine** as a bonus capability:
- Compiler oracle execution via GnuCOBOL.
- Isolated Docker sandbox (`core/sandbox.py`) for safe model code execution.
- Automated repair loops diffing stdout byte-for-byte on synthetic fixed-width inputs.
- Inspectable under the **Explorer (COBOL)** tab in `viewer/index.html`.
