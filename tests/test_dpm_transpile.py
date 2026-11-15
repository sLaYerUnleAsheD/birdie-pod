import unittest
from dpm.transpile import (
    apply_case_insensitive_compare_rule,
    apply_concat_null_semantics_rule,
    apply_integer_division_rule,
    apply_datediff_rule,
    apply_post_rules,
    build_explicit_spark_schema,
    transpile_job,
)


class TestDpmTranspile(unittest.TestCase):
    def test_case_insensitive_compare_rule(self):
        code = 'df.filter(F.col("status") == "OPEN")'
        res = apply_case_insensitive_compare_rule(code)
        self.assertIn('F.lower(F.col("status")) == F.lower(F.lit("OPEN"))', res)

    def test_concat_null_semantics_rule(self):
        code = 'SELECT ISNULL(col_a, 0) AS val'
        res = apply_concat_null_semantics_rule(code)
        self.assertIn('F.coalesce(F.col("col_a"), F.lit(0))', res)

    def test_integer_division_rule(self):
        code = 'ratio = F.col("count_a") / F.col("count_b")'
        res = apply_integer_division_rule(code)
        self.assertIn('(F.col("count_a") / F.col("count_b")).cast("bigint")', res)

    def test_datediff_rule(self):
        code = 'DATEDIFF(day, start_date, end_date)'
        res = apply_datediff_rule(code)
        self.assertEqual('F.datediff(end_date, start_date)', res)

        code_month = 'DATEDIFF(month, start_date, end_date)'
        res_month = apply_datediff_rule(code_month)
        self.assertEqual('F.floor(F.months_between(end_date, start_date))', res_month)

    def test_explicit_schema_generation(self):
        cols = {
            "policy_id": "string",
            "premium": "decimal(12,2)",
            "claim_count": "int",
        }
        schema_str = build_explicit_spark_schema(cols)
        self.assertIn('StructField("policy_id", StringType(), True)', schema_str)
        self.assertIn('StructField("premium", DecimalType(12, 2), True)', schema_str)
        self.assertIn('StructField("claim_count", IntegerType(), True)', schema_str)

    def test_transpile_job_structure(self):
        sql = "SELECT id, amount FROM stg_claims WHERE status = 'SETTLED';"
        meta = {
            "inputs": {"stg_claims": {"id": "string", "amount": "decimal(18,2)"}},
            "outputs": {"fct_settled": {"cols": {"id": "string", "amount": "decimal(18,2)"}}},
        }
        res = transpile_job(sql, meta, job_id="test_01")
        self.assertIn("def run(spark: SparkSession, in_dir: str | Path, out_dir: str | Path) -> None:", res)
        self.assertIn("schema_stg_claims", res)
        self.assertIn("createOrReplaceTempView", res)


if __name__ == "__main__":
    unittest.main()

