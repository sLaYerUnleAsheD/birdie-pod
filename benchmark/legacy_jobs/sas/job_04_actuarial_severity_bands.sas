PROC FORMAT;
    VALUE sev_fmt
        LOW - 1000      = 'ATTRITIONAL'
        1000.01 - 10000 = 'LARGE_LOSS'
        10000.01 - HIGH = 'CATASTROPHIC'
        OTHER           = 'UNKNOWN';
RUN;

DATA work.claims_banded;
    SET raw.claims;
    WHERE loss_date >= '01JAN2025'd AND claim_status NE 'OPEN';
    severity_band = PUT(reserve_amount, sev_fmt.);
    loss_year = YEAR(loss_date);
RUN;

PROC MEANS DATA=work.claims_banded NOPRINT NWAY;
    CLASS loss_year cause_code severity_band;
    VAR reserve_amount;
    OUTPUT OUT=out.severity_summary (DROP=_TYPE_ _FREQ_)
        N=claim_volume
        SUM=total_reserves
        MEAN=avg_reserve;
RUN;
