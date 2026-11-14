SELECT
    product,
    EOMONTH(MIN(inception_date)) AS month_end,
    DATEADD(day, 1, EOMONTH(MIN(inception_date))) AS next_day,
    SUM(claim_units) / NULLIF(SUM(policy_count), 0) AS claims_per_policy,
    DATEDIFF(month, MIN(inception_date), MAX(expiry_date)) AS span_months
FROM policy
GROUP BY product;
