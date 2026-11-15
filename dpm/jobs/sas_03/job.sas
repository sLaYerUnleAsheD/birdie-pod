DATA policy_claim_merged;
    MERGE policies(IN=in_p) claims(IN=in_c);
    BY policy_id;
    IF in_p AND in_c;
    has_claim = 1;
RUN;

