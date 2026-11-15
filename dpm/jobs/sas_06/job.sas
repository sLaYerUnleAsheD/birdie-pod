/* SAS sas_06: Hash-Object Deduplication + ARRAY Scoring
   Trap 1: HASH object for in-memory lookup (no SQL equivalent)
   Trap 2: ARRAY iteration over dynamic scoring coefficients
   Trap 3: RETAIN across multiple BY variables
   Trap 4: _TEMPORARY_ array (private, no output columns)
   Trap 5: DO-loop with conditional LEAVE (break)
*/

DATA scored_claims / VIEW=scored_claims;
    SET raw_claims;
    BY policy_id claim_date;

    /* Hash object: deduplicate claim_id in memory */
    IF _N_ = 1 THEN DO;
        DECLARE hash seen_claims(hashexp: 16);
        seen_claims.defineKey('claim_id');
        seen_claims.defineDone();
    END;

    IF seen_claims.check() NE 0 THEN DO;
        seen_claims.add();

        /* Scoring via ARRAY of fraud-risk coefficients */
        ARRAY coef[5] _TEMPORARY_ (0.25 0.40 0.15 0.10 0.10);
        ARRAY features[5] claim_amount_norm late_report_flag multi_policy_flag
                          prior_claims_count duplicate_address_flag;

        fraud_score = 0;
        DO i = 1 TO 5;
            IF features[i] NE . THEN
                fraud_score = fraud_score + coef[i] * features[i];
            ELSE
                fraud_score = fraud_score + 0;  /* treat missing as zero contribution */
        END;

        risk_band = IFINF(fraud_score) + 1;

        OUTPUT;
    END;
RUN;

PROC SORT DATA=scored_claims OUT=scored_sorted;
    BY DESCENDING fraud_score;
RUN;

