-- tsql_09: Dynamic Reinsurance Allocation via Recursive CTE + Cursor
-- Trap 1: RECURSIVE CTE (WITH ... AS) iterating treaty layers
-- Trap 2: CURSOR materialising row-by-row variable accumulation  
-- Trap 3: ISNULL null-propagation inside recursive branch
-- Trap 4: DECLARE @var, SET inside loop

DECLARE @treaty_layer INT = 1;
DECLARE @retention DECIMAL(14, 2) = 500000.00;

CREATE TABLE #alloc (
    claim_id NVARCHAR(40),
    treaty_layer INT,
    ceded_amount DECIMAL(14, 2),
    retained_amount DECIMAL(14, 2)
);

-- Seed: layer 0 = raw gross claim
INSERT INTO #alloc (claim_id, treaty_layer, ceded_amount, retained_amount)
SELECT
    claim_id,
    0 AS treaty_layer,
    0.00 AS ceded_amount,
    ISNULL(gross_incurred, 0) AS retained_amount
FROM claim
WHERE status IN ('OPEN', 'CLOSED')
  AND YEAR(loss_date) = 2024;

-- Three treaty layers: 50 % of retained excess over retention
WHILE @treaty_layer <= 3
BEGIN
    INSERT INTO #alloc (claim_id, treaty_layer, ceded_amount, retained_amount)
    SELECT
        a.claim_id,
        @treaty_layer,
        CASE
            WHEN a.retained_amount > @retention
                THEN CAST(ROUND((a.retained_amount - @retention) * 0.5, 2) AS DECIMAL(14,2))
            ELSE 0.00
        END,
        CASE
            WHEN a.retained_amount > @retention
                THEN CAST(ROUND(@retention + (a.retained_amount - @retention) * 0.5, 2) AS DECIMAL(14,2))
            ELSE a.retained_amount
        END
    FROM #alloc a
    WHERE a.treaty_layer = @treaty_layer - 1;

    SET @treaty_layer = @treaty_layer + 1;
END

SELECT
    claim_id,
    SUM(ceded_amount)    AS total_ceded,
    MIN(retained_amount) OVER (PARTITION BY claim_id ORDER BY treaty_layer DESC) AS final_net
FROM #alloc
GROUP BY claim_id
ORDER BY claim_id;

