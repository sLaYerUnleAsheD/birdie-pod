SELECT 
    c.claim_id,
    p.product_line,
    c.loss_date,
    c.report_date,
    DATEDIFF(DAY, c.loss_date, c.report_date) AS reporting_delay_days,
    CASE 
        WHEN DATEDIFF(DAY, c.loss_date, c.report_date) > 30 THEN 'LATE_REPORT'
        WHEN DATEDIFF(DAY, c.loss_date, c.report_date) BETWEEN 8 AND 30 THEN 'STANDARD'
        ELSE 'FAST_TRACK'
    END AS sla_bucket,
    DATEADD(DAY, 90, c.report_date) AS target_settlement_date
FROM dbo.claims c
INNER JOIN dbo.policies p ON c.policy_id = p.policy_id
WHERE c.loss_date >= '2025-01-01';
