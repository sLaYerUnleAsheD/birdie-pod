MERGE INTO policy_snapshot AS tgt
USING policy_delta AS src
ON tgt.policy_id = src.policy_id
WHEN MATCHED AND src.action = 'U' THEN
    UPDATE SET
        tgt.premium = src.premium,
        tgt.status = src.status,
        tgt.product = src.product
WHEN NOT MATCHED BY TARGET AND src.action = 'I' THEN
    INSERT (policy_id, product, premium, status)
    VALUES (src.policy_id, src.product, src.premium, src.status);

SELECT policy_id, product, premium, status
FROM policy_snapshot;
