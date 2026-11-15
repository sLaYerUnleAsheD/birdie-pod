import unittest
from decimal import Decimal
from dpm.reconcile import reconcile, Scorecard


class TestDpmReconcile(unittest.TestCase):
    def test_exact_decimal_match(self):
        exp = [
            {"id": "A1", "premium": "100.50", "units": "10"},
            {"id": "A2", "premium": "200.75", "units": "20"},
        ]
        act = [
            {"id": "A1", "premium": "100.50", "units": "10"},
            {"id": "A2", "premium": "200.75", "units": "20"},
        ]
        schema = {"id": "string", "premium": "decimal(12,2)", "units": "int"}
        score = reconcile(exp, act, keys=["id"], schema=schema, job_id="test_job", oracle_kind="sqlserver")
        self.assertTrue(score.passed)
        self.assertEqual(score.oracle_kind, "sqlserver")
        self.assertEqual(len(score.discrepancies), 0)

    def test_missing_and_extra_rows_detected(self):
        exp = [
            {"id": "A1", "val": "10"},
            {"id": "A2", "val": "20"},
        ]
        act = [
            {"id": "A2", "val": "20"},
            {"id": "A3", "val": "30"},
        ]
        schema = {"id": "string", "val": "int"}
        score = reconcile(exp, act, keys=["id"], schema=schema, job_id="test_key_diff")
        self.assertFalse(score.passed)
        self.assertFalse(score.check_cell_diff.passed)
        self.assertTrue(any(d.get("actual") == "missing" for d in score.discrepancies))
        self.assertTrue(any(d.get("expected") == "missing" for d in score.discrepancies))

    def test_decimal_rounding_and_tolerance(self):
        exp = [{"id": "A1", "amount": "10.00"}]
        act = [{"id": "A1", "amount": "10.01"}]
        schema = {"id": "string", "amount": "decimal(10,2)"}
        score = reconcile(exp, act, keys=["id"], schema=schema, job_id="test_dec")
        self.assertFalse(score.passed)
        self.assertTrue(len(score.discrepancies) > 0)
        self.assertEqual(score.discrepancies[0]["column"], "amount")

    def test_as_repair_text_output(self):
        exp = [{"id": "A1", "val": "10"}]
        act = [{"id": "A1", "val": "99"}]
        schema = {"id": "string", "val": "int"}
        score = reconcile(exp, act, keys=["id"], schema=schema, job_id="test_repair")
        repair_text = score.as_repair_text(max_chars=500)
        self.assertIn("RECONCILIATION FAILURE", repair_text)
        self.assertIn("val: expected sum 10, actual sum 99", repair_text)


if __name__ == "__main__":
    unittest.main()
