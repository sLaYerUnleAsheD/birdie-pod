---
name: modernize
description: Autonomous legacy T-SQL and SAS modernization to PySpark on Azure Lakehouse (Medallion Delta architecture) using deterministic AST parsing and Meta LLM / Claude agentic repair loops.
triggers:
  - /modernize
  - /exlake
  - /migrate
---

# EXLake.ai Modernize Skill (Meta LLM / Claude Skill)
### Agentic Legacy T-SQL & SAS to PySpark Lakehouse Modernization

Use this skill when tasked with analyzing, transpiling, repairing, or verifying legacy on-premise T-SQL ETL scripts or SAS programs into production-grade PySpark for Azure Lakehouse (Delta Lake).

---

## 1. 3-Tier Anti-Hallucination Routing Protocol

Before generating PySpark code, classify the incoming script into its minimal complexity tier:

| Tier | Characteristics | Primary Tooling | Token Cost | Parity Standard |
|---|---|---|---|---|
| **Tier A (Deterministic)** | Standard `SELECT`, `JOIN`, `WHERE`, `GROUP BY`, `#temp` staging tables. Pure `PROC SQL` in SAS. | `sqlglot` AST Transpiler | **$0.000** | 100% Deterministic AST |
| **Tier B (Hybrid)** | CTEs (`WITH`), Windowing (`OVER()`), `CROSS APPLY`, `PROC MEANS`, `PROC SORT`. | AST skeleton + Meta LLM / Claude | **~$0.015** | Parallel-run output diff |
| **Tier C (Agentic Loop)** | Procedural scripts (`WHILE`, `MERGE`, dynamic SQL, `DECLARE`), SAS `RETAIN`, `FIRST./LAST.`, `%MACRO` loops. | Autonomous Meta LLM / Claude Repair Loop | **~$0.045** | 3-artifact self-healing loop |

---

## 2. Phase 5 Semantic Guardrails (Dialect Divergence Traps)

When writing or repairing PySpark translations, you MUST adhere to these exact behavioral rules:

1. **Integer Division Truncation**:
   - In T-SQL, `int / int` performs integer floor division. In PySpark, `/` returns a float.
   - *Rule:* Always cast integer divisions: `(F.col("a") / F.col("b")).cast("integer")` or use `F.floor()`.

2. **Case-Insensitive String Comparisons**:
   - SQL Server defaults to case-insensitive collation (`CI`). Spark SQL is case-sensitive.
   - *Rule:* Apply `lower()` to both sides: `F.lower(F.col("status")) == F.lower(F.lit("ACTIVE"))`.

3. **String Concatenation & NULL Propagation**:
   - In SQL Server, `'A' + NULL` yields `NULL`. In PySpark `concat_ws` ignores nulls.
   - *Rule:* Explicitly coalesce nullable columns: `F.coalesce(F.col("col"), F.lit(""))`.

4. **Complex Decimal Arithmetic**:
   - Wrap each operand in `coalesce` *before* arithmetic operations to avoid null propagation:
     ```python
     # Correct:
     F.coalesce(F.col("reserve"), F.lit(0)) + F.coalesce(F.col("paid"), F.lit(0))
     # WRONG (nulls entire sum):
     F.coalesce(F.col("reserve") + F.col("paid"), F.lit(0))
     ```

5. **SAS Stateful `RETAIN` & `FIRST./LAST.` Translation**:
   - Never use row-by-row iteration in PySpark.
   - *Rule:* Translate to vectorized window aggregations:
     ```python
     w = Window.partitionBy("policy_id").orderBy("accident_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
     df = df.withColumn("cum_loss", F.sum("loss_amount").over(w))
     ```

---

## 3. Phase 4 Enterprise Syntactic Standards

Every generated PySpark pipeline must conform to enterprise Medallion architecture:

1. **Layer Separation**:
   - **Bronze Ingestion:** `spark.read.schema(explicit_schema).csv("...")` (Never `inferSchema=True`).
   - **Silver Conformance:** Typed joins, filtering, and column standardization.
   - **Gold Aggregation:** Final business metrics written to Delta Lake (`df.write.format("delta").mode("overwrite").saveAsTable("gold.<mart>")`).
2. **Explicit Column Aliasing**: All joined tables must be aliased (e.g. `.alias("c")` and `.alias("p")`).
3. **No Mixed Projections**: Dimension columns and aggregate metrics must strictly use `.groupBy(...).agg(...)`.

---

## 4. 4-Point Mathematical Parity Verification

Every migration must pass the 4-point verification gate:
- **Gate 1 (Row Count):** `len(legacy_df) == len(spark_df)` with zero record delta.
- **Gate 2 (Schema):** Exact column names, nullability profile, and declared decimal scales.
- **Gate 3 (Control Totals):** Exact arbitrary-precision `Decimal` equality on all financial metrics (no `np.isclose` or float drift).
- **Gate 4 (Cell Parity):** Primary key outer join diff confirming 0 cell discrepancies.

---

## 5. Execution & Verification Commands

```bash
# 1. Run live modernization pipeline on any custom T-SQL or SAS file
python scripts/modernize_on_the_fly.py <path_to_file.sql> --table gold_output_mart

# 2. Compare live SQL Server database rows vs PySpark DataFrame rows
python scripts/compare_legacy_vs_pyspark.py tsql_01

# 3. Run full automated 17-job benchmark suite
python -m dpm.benchmark --cost-cap 2.0

# 4. Launch interactive EXLake.ai Web Studio
python serve.py
```
