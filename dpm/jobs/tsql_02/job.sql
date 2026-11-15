-- SQL Server default CI collation collapses spellings of status.
-- MIN under a binary collation makes the surviving spelling deterministic.
SELECT
    region,
    MIN(status COLLATE Latin1_General_BIN) AS status,
    COUNT(*) AS claim_count,
    SUM(reserve) AS total_reserve
FROM claim
GROUP BY region, status;
