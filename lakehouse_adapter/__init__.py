"""Lakehouse Modernization Adapter for EXL Hackathon Challenge 03.
Handles 3-Tier Routing, Deterministic T-SQL/SAS AST parsing,
Lineage extraction, Data Contracts, and 4-Point Data Reconciliation.
"""

from lakehouse_adapter.router import route_legacy_script, Tier, Dialect, RoutingDecision
from lakehouse_adapter.sql_parser import parse_tsql_ast, generate_pyspark_deterministic_skeleton
from lakehouse_adapter.sas_parser import parse_sas_program
from lakehouse_adapter.reconcile import reconcile_dataframes, ReconciliationScorecard
from lakehouse_adapter.contracts import validate_dataframe_contract, CONTRACT_CLAIMS_SILVER, CONTRACT_ACTUARIAL_GOLD
from lakehouse_adapter.pipeline import run_migration_pipeline, MigrationResult
from lakehouse_adapter.benchmark_catalog import SAMPLE_JOBS, get_all_jobs

