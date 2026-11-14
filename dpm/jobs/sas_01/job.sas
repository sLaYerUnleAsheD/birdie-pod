PROC SQL;
    CREATE TABLE paid_by_lob AS
    SELECT
        lob,
        SUM(paid) AS total_paid
    FROM claims_base
    GROUP BY lob;
QUIT;

