SELECT TOP 5
    claim_id,
    policy_id,
    ISNULL(reserve, 0) AS reserve_filled
FROM claim
WHERE status = 'open'
ORDER BY ISNULL(reserve, 0) DESC, claim_id ASC;
