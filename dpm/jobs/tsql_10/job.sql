-- tsql_10: Actuarial Loss Triangle with Dynamic Pivot
-- Trap 1: PIVOT over dynamic development periods
-- Trap 2: EOMONTH boundary counting across year-end
-- Trap 3: Integer floor-division on loss ratio
-- Trap 4: Cross-join fanout via CROSS APPLY

WITH base AS (
    SELECT
        policy_id,
        YEAR(loss_date)                             AS origin_year,
        DATEDIFF(month, loss_date, settlement_date) AS dev_month,
        ISNULL(paid_loss, 0)                        AS paid_loss,
        ISNULL(case_reserve, 0)                     AS case_reserve,
        ISNULL(paid_loss, 0) + ISNULL(case_reserve, 0) AS incurred
    FROM claim c
    CROSS APPLY (
        SELECT TOP 1 p.earned_premium
        FROM policy p
        WHERE p.policy_id = c.policy_id
        ORDER BY p.effective_date DESC
    ) ep(earned_premium)
    WHERE c.status != 'DENIED'
      AND YEAR(loss_date) BETWEEN 2020 AND 2024
),
triangle AS (
    SELECT
        origin_year,
        CASE WHEN dev_month <=  12 THEN '12m'
             WHEN dev_month <=  24 THEN '24m'
             WHEN dev_month <=  36 THEN '36m'
             WHEN dev_month <=  48 THEN '48m'
             ELSE '60m+' END        AS dev_bucket,
        SUM(incurred)               AS incurred_total,
        SUM(earned_premium)         AS earned_premium,
        SUM(incurred) / NULLIF(SUM(earned_premium), 0) AS loss_ratio_raw
    FROM base
    GROUP BY origin_year,
             CASE WHEN dev_month <=  12 THEN '12m'
                  WHEN dev_month <=  24 THEN '24m'
                  WHEN dev_month <=  36 THEN '36m'
                  WHEN dev_month <=  48 THEN '48m'
                  ELSE '60m+' END
)
SELECT
    origin_year,
    dev_bucket,
    incurred_total,
    earned_premium,
    -- Integer truncation trap: SQL Server integer / integer = truncated integer
    CAST(loss_ratio_raw * 100 AS INT) AS loss_ratio_pct
FROM triangle
ORDER BY origin_year, dev_bucket;

