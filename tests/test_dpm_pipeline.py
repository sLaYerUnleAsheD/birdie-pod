import unittest
from dpm.pipeline import migrate_job


class TestDpmPipeline(unittest.TestCase):
    def test_deterministic_tier_a_zero_cost(self):
        sql = "SELECT id, amount FROM stg_claims;"
        meta = {
            "dialect": "tsql",
            "oracle_kind": "sqlserver",
            "inputs": {"stg_claims": {"id": "string", "amount": "decimal(12,2)"}},
            "outputs": {"fct_settled": {"key": ["id"], "cols": {"id": "string", "amount": "decimal(12,2)"}}},
        }
        sample_data = [{"id": "C1", "amount": "150.00"}]
        res = migrate_job("tsql_01", sql, meta, expected_data=sample_data)
        self.assertEqual(res.status, "verified")
        self.assertEqual(res.route_path, "det")
        self.assertEqual(res.total_cost_usd, 0.0)
        self.assertEqual(res.oracle_kind, "sqlserver")
        self.assertTrue(len(res.python_code) > 0)


if __name__ == "__main__":
    unittest.main()

