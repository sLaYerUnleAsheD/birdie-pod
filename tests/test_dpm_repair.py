import unittest
from unittest.mock import MagicMock
from core.llm import LLMResult
from dpm.agents import (
    META_SPARK_REPAIR_SYSTEM_PROMPT,
    format_repair_agent_input,
    clean_agent_code,
    repair_pyspark_with_meta_agent,
)
from dpm.pipeline import migrate_job


class TestDpmRepair(unittest.TestCase):
    def test_system_prompt_guardrails(self):
        # Verify Phase 5 complex math and join fidelity rules are explicitly in system prompt
        self.assertIn("Aegis Meta-Spark Repair Agent", META_SPARK_REPAIR_SYSTEM_PROMPT)
        self.assertIn("Filter Preservation", META_SPARK_REPAIR_SYSTEM_PROMPT)
        self.assertIn("Join Fidelity", META_SPARK_REPAIR_SYSTEM_PROMPT)
        self.assertIn("Complex Math Translation", META_SPARK_REPAIR_SYSTEM_PROMPT)
        self.assertIn("F.coalesce(F.col(\"A\"), F.lit(0)) + F.coalesce(F.col(\"B\"), F.lit(0))", META_SPARK_REPAIR_SYSTEM_PROMPT)
        self.assertIn("Medallion Architecture", META_SPARK_REPAIR_SYSTEM_PROMPT)
        self.assertIn("Tier C (Agentic Repair)", META_SPARK_REPAIR_SYSTEM_PROMPT)

    def test_artifact_bundling_input_schema(self):
        source = "SELECT id, ISNULL(a, 0) + ISNULL(b, 0) as total FROM t;"
        failed = "df.select((F.col('a') + F.col('b')).alias('total'))"
        err = "DataComPy Diff: 4 rows mismatched on 'total' due to null propagation."

        user_input = format_repair_agent_input(source, failed, err, job_id="tsql_repair_test")
        self.assertIn("### ARTIFACT 1: Source Legacy Code (tsql_repair_test)", user_input)
        self.assertIn("### ARTIFACT 2: Failed PySpark Attempt", user_input)
        self.assertIn("### ARTIFACT 3: Sandbox Error Report", user_input)
        self.assertIn("DataComPy Diff", user_input)

    def test_clean_agent_code_fences_and_header(self):
        raw_llm = """```python
from pyspark.sql import functions as F

def run(spark, in_dir, out_dir):
    pass
```"""
        cleaned = clean_agent_code(raw_llm)
        self.assertNotIn("```", cleaned)
        self.assertIn("def run(spark, in_dir, out_dir):", cleaned)
        self.assertIn("Tier C (Agentic Repair", cleaned)

    def test_pipeline_invokes_agent_repair_loop_on_failure(self):
        # Mock LLM Client that provides the repaired script
        mock_client = MagicMock()
        mock_result = LLMResult(
            text="""```python
from pyspark.sql import SparkSession
def run(spark, in_dir, out_dir):
    pass
```""",
            usage={"input_tokens": 400, "cached_tokens": 100, "output_tokens": 150, "reasoning_tokens": 0},
            cost_usd=0.0012,
            cached=False,
            model="muse-spark-1.3",
            provider="meta",
            list_cost_usd=0.0012,
        )
        mock_client.call.return_value = mock_result

        meta = {
            "dialect": "tsql",
            "oracle_kind": "sqlserver",
            "inputs": {"claims": {"id": "string", "paid": "decimal(12,2)"}},
            "outputs": {"mart": {"key": ["id"], "cols": {"id": "string", "paid": "decimal(12,2)"}}},
        }
        # A source that triggers Tier C (e.g. WHILE loop)
        source = "DECLARE @x INT = 1; WHILE @x < 10 BEGIN SELECT id, paid FROM claims; END"
        expected = [{"id": "C-1", "paid": "100.00"}]

        res = migrate_job(
            job_id="tsql_loop",
            source_code=source,
            meta=meta,
            expected_data=expected,
            llm_client=mock_client,
            force_agent=True,
            max_repairs=1,
        )

        self.assertTrue(mock_client.call.called)
        self.assertEqual(res.status, "verified")
        self.assertEqual(res.route_path, "agent")
        self.assertEqual(len(res.attempts), 1)
        self.assertEqual(res.attempts[0].kind, "agent_repair")
        self.assertEqual(res.attempts[0].cost_usd, 0.0012)


if __name__ == "__main__":
    unittest.main()

