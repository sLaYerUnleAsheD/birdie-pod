DATA clean_claims;
    SET claims_raw;
    IF amount > 0;
RUN;

PROC SORT DATA=clean_claims NODUPKEY;
    BY policy_id;
RUN;

PROC MEANS DATA=clean_claims NOPRINT;
    CLASS region;
    VAR amount;
    OUTPUT OUT=claims_summary(DROP=_TYPE_ _FREQ_) MEAN=avg_amount SUM=total_amount;
RUN;

