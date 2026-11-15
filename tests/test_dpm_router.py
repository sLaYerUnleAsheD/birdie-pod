import unittest
from dpm.router import route_job, Tier


class TestDpmRouter(unittest.TestCase):
    def test_tier_a_tsql(self):
        sql = """
        SELECT c.ClaimID, c.PaidAmount
        FROM dbo.Claims c
        INNER JOIN dbo.Policies p ON c.PolicyID = p.PolicyID
        WHERE c.Status = 'CLOSED';
        """
        r = route_job(sql, dialect="tsql", job_id="tsql_01")
        self.assertEqual(r.tier_code, "A")
        self.assertIn("Relational AST", r.features)

    def test_tier_b_tsql_cte_and_window(self):
        sql = """
        WITH RankedClaims AS (
            SELECT ClaimID, Amount, ROW_NUMBER() OVER (PARTITION BY PolicyID ORDER BY Amount DESC) as rn
            FROM dbo.Claims
        )
        SELECT ClaimID, Amount FROM RankedClaims WHERE rn = 1;
        """
        r = route_job(sql, dialect="tsql", job_id="tsql_04")
        self.assertEqual(r.tier_code, "B")
        self.assertTrue(any("CTE" in f or "Window" in f for f in r.features))

    def test_tier_c_tsql_procedural(self):
        sql = """
        DECLARE @Threshold DECIMAL(18,2) = 5000.00;
        SELECT * INTO #TempClaims FROM dbo.Claims WHERE Amount > @Threshold;
        WHILE (SELECT COUNT(*) FROM #TempClaims) > 0
        BEGIN
            FETCH NEXT FROM cur INTO @var;
        END
        """
        r = route_job(sql, dialect="tsql", job_id="tsql_07")
        self.assertEqual(r.tier_code, "C")
        self.assertTrue(any("WHILE" in f or "DECLARE" in f or "Temporary" in f for f in r.features))

    def test_sas_tiers(self):
        # Tier A
        sas_a = "PROC SQL; SELECT id, amount FROM claims; QUIT;"
        ra = route_job(sas_a, dialect="sas", job_id="sas_01")
        self.assertEqual(ra.tier_code, "A")

        # Tier B
        sas_b = "DATA clean; SET raw; run; PROC SORT data=clean nodupkey; by id; run;"
        rb = route_job(sas_b, dialect="sas", job_id="sas_02")
        self.assertEqual(rb.tier_code, "B")

        # Tier C
        sas_c = "DATA calc; SET grouped; by id; RETAIN cumulative_loss 0; if first.id then cumulative_loss = 0; run;"
        rc = route_job(sas_c, dialect="sas", job_id="sas_04")
        self.assertEqual(rc.tier_code, "C")


if __name__ == "__main__":
    unittest.main()

