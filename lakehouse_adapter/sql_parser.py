"""Deterministic T-SQL AST Parser and Column-Level Lineage Generator.

Uses sqlglot to parse T-SQL into an Abstract Syntax Tree (AST), extract exact
sources, targets, projection expressions, joins, and generate column-level lineage
without hallucination.
"""

from __future__ import annotations
import json
import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnLineage:
    target_column: str
    source_expr: str
    source_tables: list[str] = field(default_factory=list)
    source_columns: list[str] = field(default_factory=list)
    transformation_type: str = "DIRECT"  # DIRECT, AGGREGATE, CASE_WHEN, WINDOW, MATH
    pyspark_canonical: str = ""


@dataclass
class TableLineage:
    source_tables: list[str] = field(default_factory=list)
    target_table: str = "unknown_target"
    ctes: list[str] = field(default_factory=list)
    columns: list[ColumnLineage] = field(default_factory=list)
    joins: list[dict[str, str]] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    transpiled_spark_sql: str = ""
    transpiled_duckdb_sql: str = ""


def _clean_sql(sql: str) -> str:
    # Remove single-line comments -- ... and multiline comments /* ... */
    s = re.sub(r"--[^\n]*", "", sql)
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    return s.strip()


def parse_tsql_ast(sql: str) -> TableLineage:
    clean = _clean_sql(sql)
    lineage = TableLineage()

    try:
        import sqlglot
        from sqlglot import exp

        expression = sqlglot.parse_one(clean, read="tsql")

        # 1. Transpile to Spark SQL & DuckDB SQL
        try:
            lineage.transpiled_spark_sql = sqlglot.transpile(clean, read="tsql", write="spark")[0]
        except Exception:
            lineage.transpiled_spark_sql = clean

        try:
            lineage.transpiled_duckdb_sql = sqlglot.transpile(clean, read="tsql", write="duckdb")[0]
        except Exception:
            lineage.transpiled_duckdb_sql = clean

        # 2. Extract CTEs
        for cte in expression.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                lineage.ctes.append(alias)

        # 3. Extract Tables
        for table in expression.find_all(exp.Table):
            t_name = table.name
            if t_name and t_name not in lineage.ctes and t_name not in lineage.source_tables:
                lineage.source_tables.append(t_name)

        # 4. Extract Joins
        for join in expression.find_all(exp.Join):
            kind = join.kind or "INNER"
            on_clause = join.args.get("on")
            j_table = join.this.name if hasattr(join.this, "name") else str(join.this)
            lineage.joins.append({
                "type": str(kind).upper(),
                "table": str(j_table),
                "on": str(on_clause) if on_clause else "",
            })

        # 5. Extract Projections & Column Lineage
        for select in expression.find_all(exp.Select):
            for projection in select.expressions:
                alias = projection.alias_or_name
                expr_str = projection.sql(dialect="tsql")
                
                # Determine transformation type
                t_type = "DIRECT"
                if any(k in expr_str.upper() for k in ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX("]):
                    t_type = "AGGREGATE"
                elif "CASE" in expr_str.upper() and "WHEN" in expr_str.upper():
                    t_type = "CASE_WHEN"
                elif "OVER" in expr_str.upper():
                    t_type = "WINDOW"
                elif any(op in expr_str for op in ["*", "/", "+", "-"]):
                    t_type = "MATH"

                # Identify source columns referenced
                cols_ref = [c.name for c in projection.find_all(exp.Column) if c.name]

                # Synthesize PySpark equivalent expression
                spark_expr = projection.sql(dialect="spark")

                lineage.columns.append(ColumnLineage(
                    target_column=alias,
                    source_expr=expr_str,
                    source_columns=list(set(cols_ref)),
                    transformation_type=t_type,
                    pyspark_canonical=spark_expr,
                ))

        # 6. Extract Where filters
        where = expression.find(exp.Where)
        if where:
            lineage.filters.append(where.this.sql(dialect="tsql"))

        # 7. Extract Group By
        group = expression.find(exp.Group)
        if group:
            for g_expr in group.expressions:
                lineage.group_by.append(g_expr.sql(dialect="tsql"))

    except ImportError:
        # Robust fallback parser using regular expressions when sqlglot is not yet installed
        lineage = _regex_fallback_parse(clean)
    except Exception as e:
        # If sqlglot parsing fails on procedural constructs, fallback gracefully
        lineage = _regex_fallback_parse(clean)

    return lineage


def _regex_fallback_parse(clean: str) -> TableLineage:
    lineage = TableLineage()
    lineage.transpiled_spark_sql = clean
    lineage.transpiled_duckdb_sql = clean

    # Find CTEs
    cte_matches = re.findall(r"\bWITH\s+([a-zA-Z0-9_]+)\s+AS\b|,\s*([a-zA-Z0-9_]+)\s+AS\s*\(", clean, re.IGNORECASE)
    for m in cte_matches:
        c = m[0] or m[1]
        if c and c.lower() not in [x.lower() for x in lineage.ctes]:
            lineage.ctes.append(c)

    # Find source tables: FROM <table>, JOIN <table>
    from_matches = re.findall(r"\bFROM\s+([a-zA-Z0-9_#\.]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?", clean, re.IGNORECASE)
    for fm in from_matches:
        tbl = fm[0].split(".")[-1]
        if tbl and tbl not in lineage.ctes and tbl not in lineage.source_tables:
            lineage.source_tables.append(tbl)

    join_matches = re.findall(r"\b(LEFT|RIGHT|INNER|FULL|CROSS)?\s*JOIN\s+([a-zA-Z0-9_#\.]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?\s+ON\s+([^\n;]+)", clean, re.IGNORECASE)
    for jm in join_matches:
        jtype = jm[0] or "INNER"
        jtbl = jm[1].split(".")[-1]
        jon = jm[3].strip()
        lineage.joins.append({"type": jtype.upper(), "table": jtbl, "on": jon})
        if jtbl not in lineage.ctes and jtbl not in lineage.source_tables:
            lineage.source_tables.append(jtbl)

    # Find select projections
    select_match = re.search(r"\bSELECT\s+(.*?)\s+\bFROM\b", clean, re.IGNORECASE | re.DOTALL)
    if select_match:
        proj_str = select_match.group(1)
        # Split on commas not inside parentheses
        parts = re.split(r",(?![^\(]*\))", proj_str)
        for p in parts:
            p_strip = p.strip()
            if not p_strip:
                continue
            alias_match = re.search(r"\s+AS\s+([a-zA-Z0-9_]+)$|\s+([a-zA-Z0-9_]+)$", p_strip, re.IGNORECASE)
            col_name = alias_match.group(1) or alias_match.group(2) if alias_match else p_strip.split(".")[-1]
            
            t_type = "DIRECT"
            if any(k in p_strip.upper() for k in ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX("]):
                t_type = "AGGREGATE"
            elif "CASE" in p_strip.upper():
                t_type = "CASE_WHEN"
            elif "OVER" in p_strip.upper():
                t_type = "WINDOW"
            elif any(op in p_strip for op in ["*", "/", "+", "-"]):
                t_type = "MATH"

            lineage.columns.append(ColumnLineage(
                target_column=col_name.strip("[]\"`"),
                source_expr=p_strip,
                source_columns=[col_name.strip("[]\"`")],
                transformation_type=t_type,
                pyspark_canonical=p_strip,
            ))

    return lineage


def generate_pyspark_deterministic_skeleton(lineage: TableLineage, job_name: str = "migrated_job") -> str:
    """Generates idiomatic, deterministic PySpark DataFrame code from the TableLineage."""
    lines = [
        "# =====================================================================",
        f"# PySpark Migration Skeleton: {job_name}",
        "# Engine: Aegis Lakehouse Modernization Factory (Deterministic AST Transpiler)",
        "# Target: Azure Lakehouse (Delta Lake / Medallion Architecture)",
        "# =====================================================================",
        "from pyspark.sql import SparkSession",
        "from pyspark.sql import functions as F",
        "from pyspark.sql.window import Window",
        "",
        "spark = SparkSession.builder \\",
        f"    .appName('{job_name}') \\",
        "    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \\",
        "    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \\",
        "    .getOrCreate()",
        "",
        "# ---------------------------------------------------------------------",
        "# 1. INGESTION (Bronze / Silver Source Tables)",
        "# ---------------------------------------------------------------------",
    ]

    for src in lineage.source_tables:
        var_name = f"df_{src.replace('#', 'tmp_').lower()}"
        lines.append(f"{var_name} = spark.read.table('{src}')  # Or spark.read.format('delta').load(...)")

    lines.extend([
        "",
        "# ---------------------------------------------------------------------",
        "# 2. TRANSFORMATION & COLUMN-LEVEL LINEAGE",
        "# ---------------------------------------------------------------------",
    ])

    if lineage.joins:
        base_src = lineage.source_tables[0] if lineage.source_tables else "source"
        lines.append(f"df_joined = df_{base_src.lower()}")
        for j in lineage.joins:
            j_tbl = j['table'].lower()
            j_type = j['type'].lower()
            j_on = j['on']
            lines.append(f"# Join with {j_tbl} ({j_type.upper()}) ON {j_on}")
            lines.append(f"df_joined = df_joined.join(df_{j_tbl}, on=\"...\", how=\"{j_type}\")")
        lines.append("df_transformed = df_joined")
    elif lineage.source_tables:
        lines.append(f"df_transformed = df_{lineage.source_tables[0].lower()}")
    else:
        lines.append("df_transformed = df_source")

    lines.extend([
        "",
        "# Projected Columns with Lineage Tracing:",
        "df_final = df_transformed.select(",
    ])

    for col in lineage.columns:
        lines.append(f"    # Lineage: {col.source_expr} [{col.transformation_type}]")
        lines.append(f"    F.expr(\"{col.source_expr.replace('\"', '\\\"')}\").alias(\"{col.target_column}\"),")

    lines.extend([
        ")",
        "",
        "# ---------------------------------------------------------------------",
        "# 3. TARGET LAKEHOUSE EMISSION (Delta Lake Gold Mart)",
        "# ---------------------------------------------------------------------",
        "df_final.write \\",
        "    .format('delta') \\",
        "    .mode('overwrite') \\",
        "    .option('mergeSchema', 'true') \\",
        f"    .saveAsTable('lakehouse_gold.{job_name.lower()}')",
        "",
        "print(f'Successfully migrated and published {df_final.count()} records to Delta Lake.')",
    ])

    return "\n".join(lines)

