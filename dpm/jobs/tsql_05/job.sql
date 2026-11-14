WITH ranked AS (
    SELECT
        claim_id,
        policy_id,
        loss_date,
        reserve,
        ROW_NUMBER() OVER (
            PARTITION BY policy_id
            ORDER BY loss_date DESC, claim_id DESC
        ) AS rn
    FROM claim
)
SELECT claim_id, policy_id, loss_date, reserve
FROM ranked
WHERE rn = 1;
