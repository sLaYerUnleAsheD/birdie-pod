SELECT
    claim_id,
    policy_id,
    loss_date,
    reserve,
    SUM(reserve) OVER (
        PARTITION BY policy_id
        ORDER BY loss_date, claim_id
    ) AS running_reserve
FROM claim;
