SELECT 
    p.region,
    p.product_line,
    COUNT(DISTINCT p.policy_id) AS policy_count,
    COUNT(DISTINCT c.claim_id) AS claim_count,
    SUM(ISNULL(pay.paid_amount, 0)) AS total_incurred_paid,
    SUM(ISNULL(c.reserve_amount, 0)) AS total_open_reserves
FROM dbo.policies p
LEFT JOIN dbo.claims c ON p.policy_id = c.policy_id
LEFT JOIN dbo.payments pay ON c.claim_id = pay.claim_id
WHERE p.status IN ('ACTIVE', 'LAPSED')
GROUP BY p.region, p.product_line;
