%LET regions = DUBLIN LEINSTER MUNSTER CONNACHT;
%LET leakage_threshold = 1.25;

%MACRO run_leakage_audit;
    %LOCAL i curr_region;
    %LET i = 1;
    %LET curr_region = %SCAN(&regions, &i);
    %DO %WHILE(&curr_region NE );
        PROC SQL;
            CREATE TABLE work.audit_&curr_region AS
            SELECT 
                "&curr_region" AS audit_region LENGTH=15,
                c.claim_id,
                p.policy_id,
                c.reserve_amount,
                SUM(COALESCE(pay.paid_amount, 0)) AS actual_paid,
                CALCULATED actual_paid / NULLIF(c.reserve_amount, 0) AS paid_to_reserve_ratio
            FROM raw.policies p
            INNER JOIN raw.claims c ON p.policy_id = c.policy_id
            LEFT JOIN raw.payments pay ON c.claim_id = pay.claim_id
            WHERE p.region = "&curr_region"
              AND c.claim_status = 'CLOSED'
            GROUP BY c.claim_id, p.policy_id, c.reserve_amount
            HAVING CALCULATED paid_to_reserve_ratio > &leakage_threshold;
        QUIT;
        %LET i = %EVAL(&i + 1);
        %LET curr_region = %SCAN(&regions, &i);
    %END;
    DATA out.consolidated_leakage_flags;
        SET work.audit_DUBLIN work.audit_LEINSTER work.audit_MUNSTER work.audit_CONNACHT;
    RUN;
%MEND run_leakage_audit;
%run_leakage_audit;
