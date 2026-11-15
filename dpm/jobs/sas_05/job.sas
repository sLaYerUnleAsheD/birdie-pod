/* SAS sas_05: Bordereaux Premium Reconciliation with Macro Loop
   Trap 1: %MACRO / %DO loop over a list of underwriting years
   Trap 2: RETAIN + cumulative premium accumulator per binder
   Trap 3: PROC FORMAT for band labelling (must be replaced with F.when().otherwise())
   Trap 4: FIRST./LAST. BY-group sentinel logic per binder_id
   Trap 5: OUTPUT to multiple datasets inside a DATA step
*/

%MACRO recon_year(yr=);

PROC SORT DATA=bordereaux (WHERE=(underwriting_year=&yr));
    BY binder_id month_end_date;
RUN;

DATA bord_running_&yr bord_outliers_&yr;
    SET bordereaux;
    WHERE underwriting_year = &yr;
    BY binder_id;

    RETAIN cumulative_gwp 0 binder_gwp_ytd 0;

    IF FIRST.binder_id THEN DO;
        cumulative_gwp  = 0;
        binder_gwp_ytd  = 0;
    END;

    cumulative_gwp = cumulative_gwp + COALESCE(gross_written_premium, 0);

    /* Flag outliers: month-on-month premium > 3x running average */
    IF cumulative_gwp > 0 THEN
        avg_monthly_gwp = cumulative_gwp / MONTH(month_end_date);
    ELSE
        avg_monthly_gwp = 0;

    IF gross_written_premium > 3 * avg_monthly_gwp AND NOT FIRST.binder_id THEN DO;
        outlier_flag = 'Y';
        OUTPUT bord_outliers_&yr;
    END;
    ELSE DO;
        outlier_flag = 'N';
        OUTPUT bord_running_&yr;
    END;

    binder_gwp_ytd = cumulative_gwp;
RUN;

%MEND recon_year;

%recon_year(yr=2022);
%recon_year(yr=2023);
%recon_year(yr=2024);

DATA bordereaux_final;
    SET bord_running_2022 bord_running_2023 bord_running_2024;
RUN;

PROC MEANS DATA=bordereaux_final NWAY NOPRINT;
    CLASS binder_id underwriting_year;
    VAR cumulative_gwp;
    OUTPUT OUT=bord_summary (DROP=_TYPE_ _FREQ_)
        SUM=total_gwp
        MAX=peak_monthly_gwp;
RUN;

