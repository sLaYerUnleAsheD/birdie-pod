DECLARE @n INT = 0;

CREATE TABLE #waterfall (
    step_n INT,
    claim_id NVARCHAR(40),
    reserve DECIMAL(12, 2)
);

INSERT INTO #waterfall (step_n, claim_id, reserve)
SELECT 0, claim_id, ISNULL(reserve, 0)
FROM claim
WHERE status = 'open';

WHILE @n < 3
BEGIN
    SET @n = @n + 1;
    INSERT INTO #waterfall (step_n, claim_id, reserve)
    SELECT @n, claim_id, CAST(ROUND(reserve * 0.9, 2) AS DECIMAL(12, 2))
    FROM #waterfall
    WHERE step_n = @n - 1;
END

SELECT step_n, claim_id, reserve
FROM #waterfall;
