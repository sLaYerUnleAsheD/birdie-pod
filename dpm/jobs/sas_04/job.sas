PROC SORT DATA=claim_tx;
    BY policy_id tx_date;
RUN;

DATA claim_cumulative;
    SET claim_tx;
    BY policy_id;
    RETAIN cumulative_paid 0;
    IF FIRST.policy_id THEN cumulative_paid = 0;
    cumulative_paid = cumulative_paid + tx_amount;
RUN;

