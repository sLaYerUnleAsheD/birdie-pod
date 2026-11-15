-- tsql_11: Multi-currency FX Revaluation with sp_executesql Dynamic SQL
-- Trap 1: Dynamic SQL via sp_executesql (must escalate to Tier C)
-- Trap 2: N-ary conditional COALESCE chain on FX rate lookup
-- Trap 3: CONVERT date format with style 103 (UK dd/mm/yyyy)
-- Trap 4: TRY / CATCH error handling block

BEGIN TRY
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @currency NVARCHAR(10);
    DECLARE @rate DECIMAL(10, 6);

    -- Dynamically revalue each currency's exposure in GBP
    SET @sql = N'
        SELECT
            p.policy_id,
            p.currency,
            p.gross_written_premium                                      AS gwp_original,
            COALESCE(fx.gbp_rate, fx_fallback.gbp_rate, 1.0)            AS fx_rate,
            CAST(
                ROUND(
                    p.gross_written_premium
                    * COALESCE(fx.gbp_rate, fx_fallback.gbp_rate, 1.0),
                    2
                ) AS DECIMAL(18, 2)
            )                                                             AS gwp_gbp,
            CONVERT(NVARCHAR, GETDATE(), 103)                            AS revaluation_date
        FROM policy p
        LEFT JOIN fx_rates fx
            ON fx.currency_code = p.currency
           AND fx.rate_date = CAST(GETDATE() AS DATE)
        LEFT JOIN fx_rates fx_fallback
            ON fx_fallback.currency_code = p.currency
           AND fx_fallback.rate_date = DATEADD(day, -1, CAST(GETDATE() AS DATE))
        WHERE p.status = ''ACTIVE''
          AND p.effective_date <= CAST(GETDATE() AS DATE)
        ORDER BY p.currency, p.policy_id;
    ';

    EXEC sp_executesql @sql;

END TRY
BEGIN CATCH
    SELECT
        ERROR_NUMBER()    AS error_number,
        ERROR_MESSAGE()   AS error_message,
        ERROR_SEVERITY()  AS error_severity;
END CATCH

