"""Unit and Integration Tests for Lakehouse Modernization Engine (Challenge 03)."""

import unittest
from lakehouse_adapter.router import route_legacy_script, route_tsql, route_sas, Tier, Dialect
from lakehouse_adapter.sql_parser import parse_tsql_ast, generate_pyspark_deterministic_skeleton
from lakehouse_adapter.sas_parser import parse_sas_program
from lakehouse_adapter.reconcile import reconcile_dataframes
from lakehouse_adapter.contracts import validate_dataframe_contract, CONTRACT_CLAIMS_SILVER
from lakehouse_adapter.pipeline import run_migration_pipeline


class TestLakehouseModernization(unittest.TestCase):

    def test_tsql_routing_tier_a(self):
        sql = "SELECT id, name, amount FROM stg_customers WHERE active = 1;"
        route = route_tsql(sql)
        self.assertEqual(route.tier_code, "A")
        self.assertTrue(route.use_sqlglot_ast)
        self.assertFalse(route.use_llm_agent)

    def test_tsql_routing_tier_b(self):
        sql = """
        WITH MonthlyTotals AS (
            SELECT policy_id, SUM(prem) as total_prem FROM prem_table GROUP BY policy_id
        )
        SELECT policy_id, total_prem, ROW_NUMBER() OVER(ORDER BY total_prem DESC) as rnk
        FROM MonthlyTotals;
        """
        route = route_tsql(sql)
        self.assertEqual(route.tier_code, "B")
        self.assertTrue(route.use_sqlglot_ast)
        self.assertTrue(route.use_llm_agent)

    def test_tsql_routing_tier_c(self):
        sql = """
        DECLARE @MaxBatch INT = 50;
        SELECT * INTO #TempClaims FROM staging_claims WHERE status = 'NEW';
        WHILE @MaxBatch > 0
        BEGIN
            MERGE INTO target_claims AS t USING #TempClaims AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.flag = 1;
            SET @MaxBatch = @MaxBatch - 1;
        END;
        """
        route = route_tsql(sql)
        self.assertEqual(route.tier_code, "C")
        self.assertTrue(route.use_llm_agent)
        self.assertTrue(route.requires_repair_loop)

    def test_sas_routing_tier_a(self):
        sas = """
        PROC SQL;
            SELECT claim_id, policy_num, paid_loss FROM work.claims_in WHERE paid_loss > 1000;
        QUIT;
        """
        route = route_sas(sas)
        self.assertEqual(route.tier_code, "A")
        self.assertIsNotNone(route.inner_sql)

    def test_sas_routing_tier_b(self):
        sas = """
        DATA work.cleaned_claims;
            SET raw.claims;
            IF loss_amt < 0 THEN loss_amt = 0;
        RUN;
        PROC SORT DATA=work.cleaned_claims; BY policy_id; RUN;
        PROC MEANS DATA=work.cleaned_claims SUM; VAR loss_amt; RUN;
        """
        route = route_sas(sas)
        self.assertEqual(route.tier_code, "B")

    def test_sas_routing_tier_c(self):
        sas = """
        DATA work.triangle;
            SET work.losses;
            BY accident_year;
            RETAIN cum_loss 0;
            IF FIRST.accident_year THEN cum_loss = 0;
            cum_loss + paid_amt;
        RUN;
        """
        route = route_sas(sas)
        self.assertEqual(route.tier_code, "C")
        self.assertTrue(route.requires_repair_loop)

    def test_reconciliation_exact_parity(self):
        legacy = [
            {"claim_id": "CLM-001", "paid_amount": 1000.50, "status": "CLOSED"},
            {"claim_id": "CLM-002", "paid_amount": 2500.00, "status": "SETTLED"},
        ]
        spark = [
            {"claim_id": "CLM-001", "paid_amount": 1000.50, "status": "CLOSED"},
            {"claim_id": "CLM-002", "paid_amount": 2500.00, "status": "SETTLED"},
        ]
        scorecard = reconcile_dataframes(legacy, spark, primary_keys=["claim_id"])
        self.assertTrue(scorecard.overall_passed)
        self.assertEqual(scorecard.match_percentage, 100.0)
        self.assertTrue(scorecard.check_1_row_count.passed)
        self.assertTrue(scorecard.check_2_schema.passed)
        self.assertTrue(scorecard.check_3_control_totals.passed)
        self.assertTrue(scorecard.check_4_cell_diff.passed)

    def test_reconciliation_catches_discrepancy(self):
        legacy = [{"claim_id": "CLM-001", "paid_amount": 1000.50}]
        spark = [{"claim_id": "CLM-001", "paid_amount": 1000.80}]  # Cell diff
        scorecard = reconcile_dataframes(legacy, spark, primary_keys=["claim_id"])
        self.assertFalse(scorecard.overall_passed)
        self.assertFalse(scorecard.check_4_cell_diff.passed)

    def test_data_contract_validation(self):
        valid_data = [
            {"claim_id": "C-1", "policy_id": "P-1", "loss_date": "2023-01-01", "claim_status": "OPEN", "net_paid_amount": 100.0, "outstanding_reserve": 50.0}
        ]
        res = validate_dataframe_contract(valid_data, CONTRACT_CLAIMS_SILVER)
        self.assertTrue(res.passed)

        invalid_data = [
            {"claim_id": "C-1", "policy_id": "P-1", "loss_date": "2023-01-01", "claim_status": "INVALID_STATUS", "net_paid_amount": -50.0, "outstanding_reserve": 50.0}
        ]
        res_bad = validate_dataframe_contract(invalid_data, CONTRACT_CLAIMS_SILVER)
        self.assertFalse(res_bad.passed)


if __name__ == "__main__":
    unittest.main()


