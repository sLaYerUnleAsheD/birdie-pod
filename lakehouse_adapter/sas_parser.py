"""SAS Program Parser and Intermediate Representation Extractor.

Parses legacy SAS programs (.sas) to identify:
1. PROC SQL queries -> extract inner SQL for sqlglot translation
2. DATA steps -> extract SET sources, variable assignments, IF/THEN conditions, RETAIN state
3. PROC steps -> PROC SORT, PROC MEANS, PROC SUMMARY, PROC TRANSPOSE, PROC FORMAT
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field


@dataclass
class SasStep:
    step_type: str  # "DATA", "PROC_SQL", "PROC_SORT", "PROC_MEANS", "PROC_FORMAT", "OTHER"
    name: str = ""
    source_tables: list[str] = field(default_factory=list)
    target_table: str = ""
    raw_code: str = ""
    retain_vars: list[str] = field(default_factory=list)
    by_vars: list[str] = field(default_factory=list)
    assignments: list[dict[str, str]] = field(default_factory=list)
    inner_sql: str = ""


@dataclass
class SasProgramAnalysis:
    steps: list[SasStep] = field(default_factory=list)
    input_datasets: list[str] = field(default_factory=list)
    output_datasets: list[str] = field(default_factory=list)
    has_macro: bool = False
    has_retain: bool = False
    has_first_last: bool = False


def parse_sas_program(source: str) -> SasProgramAnalysis:
    analysis = SasProgramAnalysis()

    # Clean comments
    cleaned = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    cleaned = re.sub(r"^\s*\*.*?;", "", cleaned, flags=re.MULTILINE)

    if re.search(r"%MACRO\b", cleaned, re.IGNORECASE):
        analysis.has_macro = True
    if re.search(r"\bRETAIN\b", cleaned, re.IGNORECASE):
        analysis.has_retain = True
    if re.search(r"\bFIRST\.\w+|\bLAST\.\w+", cleaned, re.IGNORECASE):
        analysis.has_first_last = True

    # Split into steps by RUN; or QUIT;
    step_chunks = re.split(r"\b(?:RUN|QUIT)\s*;", cleaned, flags=re.IGNORECASE)

    for chunk in step_chunks:
        c = chunk.strip()
        if not c:
            continue

        # 1. PROC SQL
        proc_sql_m = re.search(r"\bPROC\s+SQL\b(.*?)$", c, re.IGNORECASE | re.DOTALL)
        if proc_sql_m:
            inner_sql = proc_sql_m.group(1).strip()
            # Extract target table if CREATE TABLE ... AS
            create_tbl = ""
            ct_m = re.search(r"\bCREATE\s+TABLE\s+([a-zA-Z0-9_\.]+)\s+AS", inner_sql, re.IGNORECASE)
            if ct_m:
                create_tbl = ct_m.group(1).split(".")[-1]
                analysis.output_datasets.append(create_tbl)

            # Extract source tables
            srcs = [s.split(".")[-1] for s in re.findall(r"\bFROM\s+([a-zA-Z0-9_\.]+)", inner_sql, re.IGNORECASE)]
            analysis.input_datasets.extend(srcs)

            analysis.steps.append(SasStep(
                step_type="PROC_SQL",
                target_table=create_tbl,
                source_tables=srcs,
                raw_code=c,
                inner_sql=inner_sql,
            ))
            continue

        # 2. DATA Step
        data_m = re.search(r"\bDATA\s+([a-zA-Z0-9_\.]+)\s*;", c, re.IGNORECASE)
        if data_m:
            target_ds = data_m.group(1).split(".")[-1]
            analysis.output_datasets.append(target_ds)

            step = SasStep(step_type="DATA", target_table=target_ds, raw_code=c)

            # SET sources
            set_m = re.findall(r"\bSET\s+([a-zA-Z0-9_\.\s]+)\s*;", c, re.IGNORECASE)
            for sm in set_m:
                for ds in sm.split():
                    clean_ds = ds.split(".")[-1]
                    step.source_tables.append(clean_ds)
                    analysis.input_datasets.append(clean_ds)

            # RETAIN variables
            retain_m = re.findall(r"\bRETAIN\s+([a-zA-Z0-9_\s]+)\s*;", c, re.IGNORECASE)
            for rm in retain_m:
                step.retain_vars.extend(rm.split())

            # BY variables
            by_m = re.findall(r"\bBY\s+([a-zA-Z0-9_\s]+)\s*;", c, re.IGNORECASE)
            for bm in by_m:
                step.by_vars.extend(bm.split())

            # Variable assignments: var = expr;
            assigns = re.findall(r"([a-zA-Z0-9_]+)\s*=\s*([^;]+);", c)
            for var, expr in assigns:
                if var.upper() not in ["DATA", "SET", "BY", "RETAIN", "IF", "FORMAT", "DROP", "KEEP"]:
                    step.assignments.append({"var": var.strip(), "expr": expr.strip()})

            analysis.steps.append(step)
            continue

        # 3. PROC SORT
        proc_sort_m = re.search(r"\bPROC\s+SORT\s+DATA\s*=\s*([a-zA-Z0-9_\.]+)(?:\s+OUT\s*=\s*([a-zA-Z0-9_\.]+))?\s*;", c, re.IGNORECASE)
        if proc_sort_m:
            in_ds = proc_sort_m.group(1).split(".")[-1]
            out_ds = (proc_sort_m.group(2) or in_ds).split(".")[-1]
            analysis.steps.append(SasStep(
                step_type="PROC_SORT",
                source_tables=[in_ds],
                target_table=out_ds,
                raw_code=c,
            ))
            continue

        # 4. PROC MEANS / SUMMARY
        proc_means_m = re.search(r"\bPROC\s+(?:MEANS|SUMMARY)\s+DATA\s*=\s*([a-zA-Z0-9_\.]+)", c, re.IGNORECASE)
        if proc_means_m:
            in_ds = proc_means_m.group(1).split(".")[-1]
            analysis.steps.append(SasStep(
                step_type="PROC_MEANS",
                source_tables=[in_ds],
                raw_code=c,
            ))
            continue

        # Other steps
        analysis.steps.append(SasStep(step_type="OTHER", raw_code=c))

    # De-duplicate
    analysis.input_datasets = list(set(analysis.input_datasets))
    analysis.output_datasets = list(set(analysis.output_datasets))

    return analysis

