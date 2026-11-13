WITH ClaimTotals AS (
    SELECT 
        c.claim_id,
        c.policy_id,
        c.loss_date,
        c.reserve_amount + ISNULL(SUM(pay.paid_amount), 0) AS total_claim_cost
    FROM dbo.claims c
    LEFT JOIN dbo.payments pay ON c.claim_id = pay.claim_id
    GROUP BY c.claim_id, c.policy_id, c.loss_date, c.reserve_amount
)
SELECT TOP 50
    ct.policy_id,
    ct.claim_id,
    ct.loss_date,
    ct.total_claim_cost,
    SUM(ct.total_claim_cost) OVER (
        PARTITION BY ct.policy_id 
        ORDER BY ct.loss_date ASC, ct.claim_id ASC
    ) AS running_policy_loss,
    ROW_NUMBER() OVER (
        PARTITION BY ct.policy_id 
        ORDER BY ct.total_claim_cost DESC, ct.claim_id ASC
    ) AS severity_rank
FROM ClaimTotals ct
ORDER BY ct.total_claim_cost DESC, ct.claim_id ASC;
