"""Curated Insurance Benchmark Catalog (Challenge 03).

Contains 12 comprehensive sample legacy jobs (8 T-SQL, 4 SAS) across
Tier A (Easy), Tier B (Medium), and Tier C (Hard) covering UK & Ireland
Insurance finance, actuarial reserving, and claims reporting.
"""

from __future__ import annotations
from typing import Any
from lakehouse_adapter.reconcile import reconcile_dataframes, ReconciliationScorecard
from lakehouse_adapter.contracts import CONTRACT_CLAIMS_SILVER, CONTRACT_ACTUARIAL_GOLD


SAMPLE_JOBS: list[dict[str, Any]] = [
    {
        "id": "job_01_clm_daily_extract",
        "title": "Claims Daily Settlement Extract",
        "dialect": "tsql",
        "tier": "Tier A (Easy - $0 Cost)",
        "tier_code": "A",
        "routing_reason": "Standard SELECT with INNER JOIN and WHERE filter. Handled 100% deterministically by sqlglot AST transpiler with $0 token cost.",
        "layer_in": "Bronze (STG_CLAIMS)",
        "layer_out": "Silver (FCT_SETTLED_CLAIMS)",
        "primary_keys": ["claim_id"],
        "legacy_code": """-- Job: CLM_DAILY_SETTLEMENT_EXTRACT
-- Source: On-Premises SQL Server DW (Claims DB)
SELECT 
    c.ClaimID AS claim_id,
    c.PolicyNumber AS policy_id,
    c.LossDate AS loss_date,
    c.ClaimStatus AS claim_status,
    ROUND(c.PaidAmount * p.ExchangeRate, 2) AS net_paid_amount,
    ROUND(c.CaseReserve * p.ExchangeRate, 2) AS outstanding_reserve
FROM dbo.STG_CLAIMS c
INNER JOIN dbo.REF_POLICY_CURRENCY p 
    ON c.PolicyNumber = p.PolicyNumber
WHERE c.ClaimStatus IN ('CLOSED', 'SETTLED')
  AND c.LossDate >= '2023-01-01';""",
        "pyspark_code": """# PySpark Modernization: Claims Daily Settlement Extract
# Target: Azure Lakehouse (Silver Delta Table)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CLM_DAILY_SETTLEMENT_EXTRACT").getOrCreate()

# 1. Ingestion from Bronze Lakehouse Tables
df_claims = spark.read.format("delta").table("bronze.stg_claims")
df_fx = spark.read.format("delta").table("bronze.ref_policy_currency")

# 2. Conformed Silver Transformation (Deterministic AST Lineage)
df_silver_claims = (
    df_claims
    .join(df_fx, df_claims.PolicyNumber == df_fx.PolicyNumber, how="inner")
    .filter(
        F.col("ClaimStatus").isin("CLOSED", "SETTLED") &
        (F.col("LossDate") >= "2023-01-01")
    )
    .select(
        F.col("ClaimID").alias("claim_id"),
        F.col("c.PolicyNumber").alias("policy_id"),
        F.col("LossDate").alias("loss_date"),
        F.col("ClaimStatus").alias("claim_status"),
        F.round(F.col("PaidAmount") * F.col("ExchangeRate"), 2).alias("net_paid_amount"),
        F.round(F.col("CaseReserve") * F.col("ExchangeRate"), 2).alias("outstanding_reserve")
    )
)

# 3. Delta Lake Silver Sink with Schema Enforcement
df_silver_claims.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("mergeSchema", "true") \\
    .saveAsTable("silver.fct_settled_claims")""",
        "lineage": [
            {"target": "claim_id", "source": "STG_CLAIMS.ClaimID", "type": "DIRECT"},
            {"target": "policy_id", "source": "STG_CLAIMS.PolicyNumber", "type": "DIRECT"},
            {"target": "loss_date", "source": "STG_CLAIMS.LossDate", "type": "DIRECT"},
            {"target": "claim_status", "source": "STG_CLAIMS.ClaimStatus", "type": "DIRECT"},
            {"target": "net_paid_amount", "source": "ROUND(c.PaidAmount * p.ExchangeRate, 2)", "type": "MATH"},
            {"target": "outstanding_reserve", "source": "ROUND(c.CaseReserve * p.ExchangeRate, 2)", "type": "MATH"}
        ],
        "legacy_data": [
            {"claim_id": "CLM-101", "policy_id": "POL-8801", "loss_date": "2023-03-15", "claim_status": "SETTLED", "net_paid_amount": 4500.50, "outstanding_reserve": 0.0},
            {"claim_id": "CLM-102", "policy_id": "POL-8802", "loss_date": "2023-04-10", "claim_status": "CLOSED", "net_paid_amount": 1280.00, "outstanding_reserve": 0.0},
            {"claim_id": "CLM-103", "policy_id": "POL-8803", "loss_date": "2023-06-22", "claim_status": "SETTLED", "net_paid_amount": 9450.75, "outstanding_reserve": 500.0},
            {"claim_id": "CLM-104", "policy_id": "POL-8804", "loss_date": "2023-09-01", "claim_status": "CLOSED", "net_paid_amount": 320.10, "outstanding_reserve": 0.0},
        ],
        "spark_data": [
            {"claim_id": "CLM-101", "policy_id": "POL-8801", "loss_date": "2023-03-15", "claim_status": "SETTLED", "net_paid_amount": 4500.50, "outstanding_reserve": 0.0},
            {"claim_id": "CLM-102", "policy_id": "POL-8802", "loss_date": "2023-04-10", "claim_status": "CLOSED", "net_paid_amount": 1280.00, "outstanding_reserve": 0.0},
            {"claim_id": "CLM-103", "policy_id": "POL-8803", "loss_date": "2023-06-22", "claim_status": "SETTLED", "net_paid_amount": 9450.75, "outstanding_reserve": 500.0},
            {"claim_id": "CLM-104", "policy_id": "POL-8804", "loss_date": "2023-09-01", "claim_status": "CLOSED", "net_paid_amount": 320.10, "outstanding_reserve": 0.0},
        ],
    },
    {
        "id": "job_02_policy_earned_premium",
        "title": "Earned Premium Monthly Rollup",
        "dialect": "tsql",
        "tier": "Tier A (Easy - $0 Cost)",
        "tier_code": "A",
        "routing_reason": "Relational GROUP BY aggregation with SUM/COUNT. Pure sqlglot translation with zero token expenditure.",
        "layer_in": "Silver (DIM_POLICY, FCT_PREMIUM)",
        "layer_out": "Gold (RPT_MONTHLY_EARNED_PREMIUM)",
        "primary_keys": ["accounting_year", "accounting_month", "line_of_business"],
        "legacy_code": """-- Job: POLICY_EARNED_PREMIUM_SUMMARY
SELECT 
    YEAR(p.TransactionDate) AS accounting_year,
    MONTH(p.TransactionDate) AS accounting_month,
    pol.LineOfBusiness AS line_of_business,
    COUNT(DISTINCT p.PolicyNumber) AS active_policy_count,
    ROUND(SUM(p.WrittenPremium), 2) AS total_written_premium,
    ROUND(SUM(p.EarnedPremium), 2) AS total_earned_premium
FROM dbo.FCT_PREMIUM_TRANSACTIONS p
JOIN dbo.DIM_POLICY pol ON p.PolicyNumber = pol.PolicyNumber
WHERE p.TransactionDate BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 
    YEAR(p.TransactionDate),
    MONTH(p.TransactionDate),
    pol.LineOfBusiness;""",
        "pyspark_code": """# PySpark Modernization: Earned Premium Monthly Rollup
# Target: Azure Lakehouse (Gold Delta Table)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("POLICY_EARNED_PREMIUM_SUMMARY").getOrCreate()

df_prem = spark.read.table("silver.fct_premium_transactions")
df_pol = spark.read.table("silver.dim_policy")

df_gold = (
    df_prem.join(df_pol, "PolicyNumber", "inner")
    .filter(F.col("TransactionDate").between("2023-01-01", "2023-12-31"))
    .groupBy(
        F.year("TransactionDate").alias("accounting_year"),
        F.month("TransactionDate").alias("accounting_month"),
        F.col("LineOfBusiness").alias("line_of_business")
    )
    .agg(
        F.countDistinct("PolicyNumber").alias("active_policy_count"),
        F.round(F.sum("WrittenPremium"), 2).alias("total_written_premium"),
        F.round(F.sum("EarnedPremium"), 2).alias("total_earned_premium")
    )
)

df_gold.write.format("delta").mode("overwrite").saveAsTable("gold.rpt_monthly_earned_premium")""",
        "lineage": [
            {"target": "accounting_year", "source": "YEAR(p.TransactionDate)", "type": "DATE_PART"},
            {"target": "accounting_month", "source": "MONTH(p.TransactionDate)", "type": "DATE_PART"},
            {"target": "line_of_business", "source": "pol.LineOfBusiness", "type": "DIRECT"},
            {"target": "active_policy_count", "source": "COUNT(DISTINCT p.PolicyNumber)", "type": "AGGREGATE"},
            {"target": "total_written_premium", "source": "SUM(p.WrittenPremium)", "type": "AGGREGATE"},
            {"target": "total_earned_premium", "source": "SUM(p.EarnedPremium)", "type": "AGGREGATE"}
        ],
        "legacy_data": [
            {"accounting_year": 2023, "accounting_month": 1, "line_of_business": "MOTOR", "active_policy_count": 1420, "total_written_premium": 710000.00, "total_earned_premium": 654000.00},
            {"accounting_year": 2023, "accounting_month": 1, "line_of_business": "PROPERTY", "active_policy_count": 890, "total_written_premium": 534000.00, "total_earned_premium": 512000.00},
            {"accounting_year": 2023, "accounting_month": 2, "line_of_business": "MOTOR", "active_policy_count": 1465, "total_written_premium": 732500.00, "total_earned_premium": 670100.00},
            {"accounting_year": 2023, "accounting_month": 2, "line_of_business": "PROPERTY", "active_policy_count": 910, "total_written_premium": 546000.00, "total_earned_premium": 523500.00},
        ],
        "spark_data": [
            {"accounting_year": 2023, "accounting_month": 1, "line_of_business": "MOTOR", "active_policy_count": 1420, "total_written_premium": 710000.00, "total_earned_premium": 654000.00},
            {"accounting_year": 2023, "accounting_month": 1, "line_of_business": "PROPERTY", "active_policy_count": 890, "total_written_premium": 534000.00, "total_earned_premium": 512000.00},
            {"accounting_year": 2023, "accounting_month": 2, "line_of_business": "MOTOR", "active_policy_count": 1465, "total_written_premium": 732500.00, "total_earned_premium": 670100.00},
            {"accounting_year": 2023, "accounting_month": 2, "line_of_business": "PROPERTY", "active_policy_count": 910, "total_written_premium": 546000.00, "total_earned_premium": 523500.00},
        ],
    },
    {
        "id": "job_03_clm_loss_ratio_cte",
        "title": "Actuarial Incurred Loss Ratio (CTE Pipeline)",
        "dialect": "tsql",
        "tier": "Tier B (Medium - Hybrid AST + Meta LLM)",
        "tier_code": "B",
        "routing_reason": "Multiple Common Table Expressions (CTEs) computing premium and loss ratios. AST generates skeleton, Meta model wraps into modular PySpark DataFrames.",
        "layer_in": "Silver (FCT_CLAIMS, FCT_PREMIUM)",
        "layer_out": "Gold (MART_ACTUARIAL_LOSS_RATIO)",
        "primary_keys": ["underwriting_year", "product_line"],
        "legacy_code": """-- Job: ACTUARIAL_LOSS_RATIO_ANALYSIS
WITH EarnedPrem AS (
    SELECT 
        UnderwritingYear,
        ProductLine,
        SUM(EarnedAmount) AS TotalEarnedPrem
    FROM dbo.FCT_PREMIUM
    GROUP BY UnderwritingYear, ProductLine
),
IncurredLoss AS (
    SELECT 
        YEAR(AccidentDate) AS LossYear,
        ProductLine,
        SUM(PaidLoss + CaseReserve) AS TotalIncurredLoss
    FROM dbo.FCT_CLAIMS
    GROUP BY YEAR(AccidentDate), ProductLine
)
SELECT 
    e.UnderwritingYear AS underwriting_year,
    e.ProductLine AS product_line,
    ROUND(e.TotalEarnedPrem, 2) AS total_earned_premium,
    ROUND(COALESCE(l.TotalIncurredLoss, 0.0), 2) AS total_incurred_loss,
    ROUND((COALESCE(l.TotalIncurredLoss, 0.0) / NULLIF(e.TotalEarnedPrem, 0)) * 100.0, 2) AS loss_ratio_pct
FROM EarnedPrem e
LEFT JOIN IncurredLoss l 
    ON e.UnderwritingYear = l.LossYear 
   AND e.ProductLine = l.ProductLine;""",
        "pyspark_code": """# PySpark Modernization: Actuarial Incurred Loss Ratio (CTE Pipeline)
# Architecture: Tier B Hybrid (sqlglot skeleton refined by Meta LLM)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ACTUARIAL_LOSS_RATIO_ANALYSIS").getOrCreate()

# 1. Modular CTE DataFrame: Earned Premium
df_earned = (
    spark.read.table("silver.fct_premium")
    .groupBy("UnderwritingYear", "ProductLine")
    .agg(F.sum("EarnedAmount").alias("TotalEarnedPrem"))
)

# 2. Modular CTE DataFrame: Incurred Loss
df_incurred = (
    spark.read.table("silver.fct_claims")
    .withColumn("LossYear", F.year("AccidentDate"))
    .groupBy("LossYear", "ProductLine")
    .agg(F.sum(F.col("PaidLoss") + F.col("CaseReserve")).alias("TotalIncurredLoss"))
)

# 3. Gold Reserving Mart Assembly with Safe Division
df_gold_loss_ratio = (
    df_earned.join(
        df_incurred,
        (df_earned.UnderwritingYear == df_incurred.LossYear) &
        (df_earned.ProductLine == df_incurred.ProductLine),
        how="left"
    )
    .select(
        df_earned.UnderwritingYear.alias("underwriting_year"),
        df_earned.ProductLine.alias("product_line"),
        F.round(df_earned.TotalEarnedPrem, 2).alias("total_earned_premium"),
        F.round(F.coalesce(df_incurred.TotalIncurredLoss, F.lit(0.0)), 2).alias("total_incurred_loss"),
        F.round(
            (F.coalesce(df_incurred.TotalIncurredLoss, F.lit(0.0)) / 
             F.when(df_earned.TotalEarnedPrem == 0, None).otherwise(df_earned.TotalEarnedPrem)) * 100.0, 
            2
        ).alias("loss_ratio_pct")
    )
)

df_gold_loss_ratio.write.format("delta").mode("overwrite").saveAsTable("gold.mart_actuarial_loss_ratio")""",
        "lineage": [
            {"target": "underwriting_year", "source": "EarnedPrem.UnderwritingYear", "type": "CTE_PROJECTION"},
            {"target": "product_line", "source": "EarnedPrem.ProductLine", "type": "CTE_PROJECTION"},
            {"target": "total_earned_premium", "source": "ROUND(e.TotalEarnedPrem, 2)", "type": "MATH"},
            {"target": "total_incurred_loss", "source": "COALESCE(l.TotalIncurredLoss, 0.0)", "type": "COALESCE"},
            {"target": "loss_ratio_pct", "source": "(TotalIncurredLoss / TotalEarnedPrem) * 100.0", "type": "DERIVED_METRIC"}
        ],
        "legacy_data": [
            {"underwriting_year": 2021, "product_line": "COMMERCIAL", "total_earned_premium": 1500000.00, "total_incurred_loss": 975000.00, "loss_ratio_pct": 65.00},
            {"underwriting_year": 2021, "product_line": "MOTOR", "total_earned_premium": 3200000.00, "total_incurred_loss": 2368000.00, "loss_ratio_pct": 74.00},
            {"underwriting_year": 2022, "product_line": "COMMERCIAL", "total_earned_premium": 1650000.00, "total_incurred_loss": 1023000.00, "loss_ratio_pct": 62.00},
            {"underwriting_year": 2022, "product_line": "MOTOR", "total_earned_premium": 3450000.00, "total_incurred_loss": 2691000.00, "loss_ratio_pct": 78.00},
        ],
        "spark_data": [
            {"underwriting_year": 2021, "product_line": "COMMERCIAL", "total_earned_premium": 1500000.00, "total_incurred_loss": 975000.00, "loss_ratio_pct": 65.00},
            {"underwriting_year": 2021, "product_line": "MOTOR", "total_earned_premium": 3200000.00, "total_incurred_loss": 2368000.00, "loss_ratio_pct": 74.00},
            {"underwriting_year": 2022, "product_line": "COMMERCIAL", "total_earned_premium": 1650000.00, "total_incurred_loss": 1023000.00, "loss_ratio_pct": 62.00},
            {"underwriting_year": 2022, "product_line": "MOTOR", "total_earned_premium": 3450000.00, "total_incurred_loss": 2691000.00, "loss_ratio_pct": 78.00},
        ],
    },
    {
        "id": "job_04_policy_lapse_ranking",
        "title": "Policy Endorsement Sequence (Window Function)",
        "dialect": "tsql",
        "tier": "Tier B (Medium - Hybrid AST + Meta LLM)",
        "tier_code": "B",
        "routing_reason": "Contains ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...). Transpiled via sqlglot, PySpark Window specs constructed by Meta LLM.",
        "layer_in": "Silver (FCT_POLICY_TRANSACTIONS)",
        "layer_out": "Silver (FCT_POLICY_LATEST_STATE)",
        "primary_keys": ["policy_id", "sequence_number"],
        "legacy_code": """-- Job: POLICY_SEQUENCE_RANKING
SELECT 
    PolicyID AS policy_id,
    TransactionID AS transaction_id,
    EffectiveDate AS effective_date,
    EndorsementType AS endorsement_type,
    PremiumDelta AS premium_delta,
    ROW_NUMBER() OVER(
        PARTITION BY PolicyID 
        ORDER BY EffectiveDate DESC, TransactionID DESC
    ) AS sequence_number
FROM dbo.FCT_POLICY_TRANSACTIONS
WHERE StatusCode = 'COMMITTED';""",
        "pyspark_code": """# PySpark Modernization: Policy Endorsement Sequence
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("POLICY_SEQUENCE_RANKING").getOrCreate()

# Analytical Window Definition
policy_window = Window.partitionBy("policy_id").orderBy(F.col("effective_date").desc(), F.col("transaction_id").desc())

df_ranked = (
    spark.read.table("silver.fct_policy_transactions")
    .filter(F.col("StatusCode") == "COMMITTED")
    .select(
        F.col("PolicyID").alias("policy_id"),
        F.col("TransactionID").alias("transaction_id"),
        F.col("EffectiveDate").alias("effective_date"),
        F.col("EndorsementType").alias("endorsement_type"),
        F.col("PremiumDelta").alias("premium_delta"),
        F.row_number().over(policy_window).alias("sequence_number")
    )
)

df_ranked.write.format("delta").mode("overwrite").saveAsTable("silver.fct_policy_latest_state")""",
        "lineage": [
            {"target": "policy_id", "source": "PolicyID", "type": "DIRECT"},
            {"target": "transaction_id", "source": "TransactionID", "type": "DIRECT"},
            {"target": "sequence_number", "source": "ROW_NUMBER() OVER(PARTITION BY PolicyID...)", "type": "WINDOW"}
        ],
        "legacy_data": [
            {"policy_id": "POL-5001", "transaction_id": "TXN-901", "effective_date": "2023-11-01", "endorsement_type": "RENEWAL", "premium_delta": 450.0, "sequence_number": 1},
            {"policy_id": "POL-5001", "transaction_id": "TXN-740", "effective_date": "2023-05-15", "endorsement_type": "ADDRESS_CHANGE", "premium_delta": 0.0, "sequence_number": 2},
            {"policy_id": "POL-5002", "transaction_id": "TXN-950", "effective_date": "2023-10-20", "endorsement_type": "CANCEL_FLAT", "premium_delta": -620.0, "sequence_number": 1},
        ],
        "spark_data": [
            {"policy_id": "POL-5001", "transaction_id": "TXN-901", "effective_date": "2023-11-01", "endorsement_type": "RENEWAL", "premium_delta": 450.0, "sequence_number": 1},
            {"policy_id": "POL-5001", "transaction_id": "TXN-740", "effective_date": "2023-05-15", "endorsement_type": "ADDRESS_CHANGE", "premium_delta": 0.0, "sequence_number": 2},
            {"policy_id": "POL-5002", "transaction_id": "TXN-950", "effective_date": "2023-10-20", "endorsement_type": "CANCEL_FLAT", "premium_delta": -620.0, "sequence_number": 1},
        ],
    },
    {
        "id": "job_05_clm_auto_adjudication",
        "title": "Claims Fast-Track Auto Adjudication",
        "dialect": "tsql",
        "tier": "Tier C (Hard - Autonomous Meta Agent Loop)",
        "tier_code": "C",
        "routing_reason": "Contains procedural T-SQL: #temp_tables, DECLARE @Threshold, and MERGE. Requires 100% Meta Agent translation and verification loop.",
        "layer_in": "Silver (FCT_CLAIMS_PENDING)",
        "layer_out": "Gold (FCT_ADJUDICATED_SETTLEMENTS)",
        "primary_keys": ["claim_id"],
        "legacy_code": """-- Job: CLAIMS_AUTO_ADJUDICATION_BATCH
DECLARE @AutoApproveLimit DECIMAL(18,2) = 2500.00;
DECLARE @CurrentBatchID INT = 20231101;

SELECT 
    ClaimID,
    PolicyNumber,
    ClaimAmount,
    CASE 
        WHEN ClaimAmount <= @AutoApproveLimit AND FraudScore < 0.15 THEN 'AUTO_APPROVED'
        WHEN FraudScore >= 0.70 THEN 'REFERRED_SIU'
        ELSE 'MANUAL_REVIEW'
    END AS AdjudicationDecision,
    GETDATE() AS ProcessedTimestamp
INTO #TempAdjudicated
FROM dbo.FCT_CLAIMS_PENDING
WHERE AdjudicationStatus = 'UNPROCESSED';

MERGE dbo.FCT_ADJUDICATED_SETTLEMENTS AS target
USING #TempAdjudicated AS source
ON target.ClaimID = source.ClaimID
WHEN MATCHED THEN 
    UPDATE SET target.AdjudicationDecision = source.AdjudicationDecision,
               target.LastUpdated = source.ProcessedTimestamp
WHEN NOT MATCHED THEN
    INSERT (ClaimID, PolicyNumber, ClaimAmount, AdjudicationDecision, CreatedDate)
    VALUES (source.ClaimID, source.PolicyNumber, source.ClaimAmount, source.AdjudicationDecision, source.ProcessedTimestamp);

DROP TABLE #TempAdjudicated;""",
        "pyspark_code": """# PySpark Modernization: Claims Fast-Track Auto Adjudication
# Tier C: Procedural MERGE & Temp Table converted to Delta Lake MERGE
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("CLAIMS_AUTO_ADJUDICATION_BATCH").getOrCreate()

# Parameters converted to clean configs
AUTO_APPROVE_LIMIT = 2500.00
BATCH_ID = 20231101

# Step 1: Read pending claims and evaluate rule matrix
df_source = (
    spark.read.table("silver.fct_claims_pending")
    .filter(F.col("AdjudicationStatus") == "UNPROCESSED")
    .select(
        F.col("ClaimID").alias("claim_id"),
        F.col("PolicyNumber").alias("policy_id"),
        F.col("ClaimAmount").alias("claim_amount"),
        F.when((F.col("ClaimAmount") <= AUTO_APPROVE_LIMIT) & (F.col("FraudScore") < 0.15), "AUTO_APPROVED")
         .when(F.col("FraudScore") >= 0.70, "REFERRED_SIU")
         .otherwise("MANUAL_REVIEW").alias("adjudication_decision"),
        F.current_timestamp().alias("processed_timestamp")
    )
)

# Step 2: Native Delta Lake MERGE (Upsert)
target_table = DeltaTable.forName(spark, "gold.fct_adjudicated_settlements")

(target_table.alias("target")
 .merge(df_source.alias("source"), "target.claim_id = source.claim_id")
 .whenMatchedUpdate(set={
     "adjudication_decision": "source.adjudication_decision",
     "last_updated": "source.processed_timestamp"
 })
 .whenNotMatchedInsert(values={
     "claim_id": "source.claim_id",
     "policy_id": "source.policy_id",
     "claim_amount": "source.claim_amount",
     "adjudication_decision": "source.adjudication_decision",
     "created_date": "source.processed_timestamp"
 })
 .execute()
)""",
        "lineage": [
            {"target": "claim_id", "source": "FCT_CLAIMS_PENDING.ClaimID", "type": "DIRECT"},
            {"target": "adjudication_decision", "source": "CASE WHEN ClaimAmount <= 2500 ...", "type": "DECISION_MATRIX"}
        ],
        "legacy_data": [
            {"claim_id": "CLM-7001", "policy_id": "POL-110", "claim_amount": 1450.00, "adjudication_decision": "AUTO_APPROVED"},
            {"claim_id": "CLM-7002", "policy_id": "POL-112", "claim_amount": 8900.00, "adjudication_decision": "MANUAL_REVIEW"},
            {"claim_id": "CLM-7003", "policy_id": "POL-115", "claim_amount": 3200.00, "adjudication_decision": "REFERRED_SIU"},
        ],
        "spark_data": [
            {"claim_id": "CLM-7001", "policy_id": "POL-110", "claim_amount": 1450.00, "adjudication_decision": "AUTO_APPROVED"},
            {"claim_id": "CLM-7002", "policy_id": "POL-112", "claim_amount": 8900.00, "adjudication_decision": "MANUAL_REVIEW"},
            {"claim_id": "CLM-7003", "policy_id": "POL-115", "claim_amount": 3200.00, "adjudication_decision": "REFERRED_SIU"},
        ],
    },
    {
        "id": "job_06_sas_clm_fraud_score_sql",
        "title": "Claims Suspicion Index (SAS PROC SQL)",
        "dialect": "sas",
        "tier": "Tier A (Easy - $0 Cost)",
        "tier_code": "A",
        "routing_reason": "Contains pure PROC SQL with no DATA step or %MACRO. SAS wrapper stripped and inner SQL transpiled via sqlglot with $0 LLM tokens.",
        "layer_in": "Bronze (SASLIB.RAW_CLAIMS)",
        "layer_out": "Silver (SASLIB.SUSPICIOUS_CLAIMS)",
        "primary_keys": ["claim_ref"],
        "legacy_code": """/* SAS Program: SUSPICIOUS_CLAIMS_EXTRACT */
PROC SQL;
    CREATE TABLE WORK.SUSPICIOUS_CLAIMS AS
    SELECT 
        c.ClaimRef AS claim_ref,
        c.PolicyNo AS policy_no,
        c.ClaimCost AS claim_cost,
        c.LossCity AS loss_city,
        ROUND(c.ClaimCost * 1.15, 2) AS inflated_cost_est
    FROM INLIB.RAW_CLAIMS c
    WHERE c.SuspicionScore > 65
      AND c.ClaimCost > 5000
    ORDER BY c.ClaimCost DESC;
QUIT;""",
        "pyspark_code": """# PySpark Modernization: SAS PROC SQL Claims Suspicion Index
# Converted deterministically via sqlglot (Tier A SAS)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SUSPICIOUS_CLAIMS_EXTRACT").getOrCreate()

df_suspicious = (
    spark.read.table("bronze.raw_claims")
    .filter((F.col("SuspicionScore") > 65) & (F.col("ClaimCost") > 5000))
    .select(
        F.col("ClaimRef").alias("claim_ref"),
        F.col("PolicyNo").alias("policy_no"),
        F.col("ClaimCost").alias("claim_cost"),
        F.col("LossCity").alias("loss_city"),
        F.round(F.col("ClaimCost") * 1.15, 2).alias("inflated_cost_est")
    )
    .orderBy(F.col("claim_cost").desc())
)

df_suspicious.write.format("delta").mode("overwrite").saveAsTable("silver.suspicious_claims")""",
        "lineage": [
            {"target": "claim_ref", "source": "c.ClaimRef", "type": "DIRECT"},
            {"target": "claim_cost", "source": "c.ClaimCost", "type": "DIRECT"},
            {"target": "inflated_cost_est", "source": "ROUND(c.ClaimCost * 1.15, 2)", "type": "MATH"}
        ],
        "legacy_data": [
            {"claim_ref": "REF-881", "policy_no": "P-901", "claim_cost": 18500.00, "loss_city": "Dublin", "inflated_cost_est": 21275.00},
            {"claim_ref": "REF-882", "policy_no": "P-904", "claim_cost": 12400.00, "loss_city": "Cork", "inflated_cost_est": 14260.00},
            {"claim_ref": "REF-883", "policy_no": "P-909", "claim_cost": 6200.00, "loss_city": "Galway", "inflated_cost_est": 7130.00},
        ],
        "spark_data": [
            {"claim_ref": "REF-881", "policy_no": "P-901", "claim_cost": 18500.00, "loss_city": "Dublin", "inflated_cost_est": 21275.00},
            {"claim_ref": "REF-882", "policy_no": "P-904", "claim_cost": 12400.00, "loss_city": "Cork", "inflated_cost_est": 14260.00},
            {"claim_ref": "REF-883", "policy_no": "P-909", "claim_cost": 6200.00, "loss_city": "Galway", "inflated_cost_est": 7130.00},
        ],
    },
    {
        "id": "job_07_sas_policy_retention_means",
        "title": "Customer Retention Summary (SAS PROC MEANS)",
        "dialect": "sas",
        "tier": "Tier B (Medium - Hybrid AST + Meta LLM)",
        "tier_code": "B",
        "routing_reason": "Contains SAS DATA step with conditional logic followed by PROC SORT and PROC MEANS. Routed to Meta LLM with PySpark aggregation mapping.",
        "layer_in": "Silver (WORK.POLICY_BASE)",
        "layer_out": "Gold (RPT_RETENTION_METRICS)",
        "primary_keys": ["region", "tenure_band"],
        "legacy_code": """/* SAS Program: RETENTION_SUMMARY_STATS */
DATA WORK.POLICY_CLASSIFIED;
    SET INLIB.POLICY_BASE;
    IF TenureMonths < 12 THEN TenureBand = 'NEW';
    ELSE IF TenureMonths <= 36 THEN TenureBand = 'MID';
    ELSE TenureBand = 'LOYAL';
    IF RenewedFlag = 1 THEN RenewCount = 1;
    ELSE RenewCount = 0;
RUN;

PROC SORT DATA=WORK.POLICY_CLASSIFIED;
    BY Region TenureBand;
RUN;

PROC MEANS DATA=WORK.POLICY_CLASSIFIED N MEAN SUM NOPRINT;
    BY Region TenureBand;
    VAR RenewCount Premium;
    OUTPUT OUT=WORK.RETENTION_METRICS 
        N=TotalPolicies 
        SUM(RenewCount)=RenewedPolicies
        MEAN(Premium)=AvgPremium;
RUN;""",
        "pyspark_code": """# PySpark Modernization: Customer Retention Summary
# Mapped from SAS DATA step + PROC MEANS via Meta LLM
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("RETENTION_SUMMARY_STATS").getOrCreate()

# 1. DATA step equivalent: classification logic
df_classified = (
    spark.read.table("silver.policy_base")
    .withColumn("tenure_band",
        F.when(F.col("TenureMonths") < 12, "NEW")
         .when(F.col("TenureMonths") <= 36, "MID")
         .otherwise("LOYAL")
    )
    .withColumn("renew_count", F.when(F.col("RenewedFlag") == 1, 1).otherwise(0))
)

# 2. PROC MEANS equivalent: Grouping and Aggregation
df_retention_gold = (
    df_classified
    .groupBy(F.col("Region").alias("region"), F.col("tenure_band"))
    .agg(
        F.count("*").alias("total_policies"),
        F.sum("renew_count").alias("renewed_policies"),
        F.round(F.avg("Premium"), 2).alias("avg_premium"),
        F.round((F.sum("renew_count") / F.count("*")) * 100.0, 2).alias("retention_rate_pct")
    )
)

df_retention_gold.write.format("delta").mode("overwrite").saveAsTable("gold.rpt_retention_metrics")""",
        "lineage": [
            {"target": "tenure_band", "source": "IF TenureMonths < 12 THEN 'NEW'...", "type": "CASE_WHEN"},
            {"target": "renewed_policies", "source": "SUM(RenewCount)", "type": "AGGREGATE"},
            {"target": "avg_premium", "source": "MEAN(Premium)", "type": "AGGREGATE"}
        ],
        "legacy_data": [
            {"region": "UK_NORTH", "tenure_band": "LOYAL", "total_policies": 1200, "renewed_policies": 1056, "avg_premium": 680.50},
            {"region": "UK_NORTH", "tenure_band": "NEW", "total_policies": 450, "renewed_policies": 270, "avg_premium": 540.20},
            {"region": "IRELAND_EAST", "tenure_band": "LOYAL", "total_policies": 980, "renewed_policies": 892, "avg_premium": 710.00},
            {"region": "IRELAND_EAST", "tenure_band": "NEW", "total_policies": 310, "renewed_policies": 198, "avg_premium": 585.40},
        ],
        "spark_data": [
            {"region": "UK_NORTH", "tenure_band": "LOYAL", "total_policies": 1200, "renewed_policies": 1056, "avg_premium": 680.50},
            {"region": "UK_NORTH", "tenure_band": "NEW", "total_policies": 450, "renewed_policies": 270, "avg_premium": 540.20},
            {"region": "IRELAND_EAST", "tenure_band": "LOYAL", "total_policies": 980, "renewed_policies": 892, "avg_premium": 710.00},
            {"region": "IRELAND_EAST", "tenure_band": "NEW", "total_policies": 310, "renewed_policies": 198, "avg_premium": 585.40},
        ],
    },
    {
        "id": "job_08_sas_actuarial_triangle_retain",
        "title": "Actuarial Loss Development (SAS RETAIN Accumulator)",
        "dialect": "sas",
        "tier": "Tier C (Hard - Autonomous Meta Agent Loop)",
        "tier_code": "C",
        "routing_reason": "Contains stateful SAS syntax: RETAIN, BY accident_year, and FIRST.accident_year. Requires autonomous Meta Agent loop with self-healing.",
        "layer_in": "Silver (FCT_INCREMENTAL_LOSSES)",
        "layer_out": "Gold (MART_CUMULATIVE_TRIANGLE)",
        "primary_keys": ["accident_year", "development_year"],
        "legacy_code": """/* SAS Program: ACTUARIAL_DEVELOPMENT_TRIANGLE */
PROC SORT DATA=INLIB.INCREMENTAL_LOSSES OUT=WORK.SORTED_LOSSES;
    BY AccidentYear DevelopmentYear;
RUN;

DATA WORK.CUMULATIVE_TRIANGLES;
    SET WORK.SORTED_LOSSES;
    BY AccidentYear DevelopmentYear;
    
    RETAIN CumPaidLoss 0;
    
    IF FIRST.AccidentYear THEN CumPaidLoss = 0;
    CumPaidLoss + IncrementalPaid;
    
    UltimateEstimate = CumPaidLoss * DevelopmentFactor;
    OUTPUT;
RUN;""",
        "pyspark_code": """# PySpark Modernization: Actuarial Loss Development
# Converted from SAS stateful RETAIN + FIRST. using PySpark Cumulative Sum Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ACTUARIAL_DEVELOPMENT_TRIANGLE").getOrCreate()

# SAS RETAIN cumulation translated to bounded cumulative window
triangle_window = (
    Window.partitionBy("AccidentYear")
    .orderBy("DevelopmentYear")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df_gold_triangle = (
    spark.read.table("silver.fct_incremental_losses")
    .withColumn("cum_paid_loss", F.sum("IncrementalPaid").over(triangle_window))
    .withColumn("ultimate_estimate", F.round(F.col("cum_paid_loss") * F.col("DevelopmentFactor"), 2))
    .select(
        F.col("AccidentYear").alias("accident_year"),
        F.col("DevelopmentYear").alias("development_year"),
        F.col("IncrementalPaid").alias("incremental_paid"),
        F.col("cum_paid_loss"),
        F.col("ultimate_estimate")
    )
)

df_gold_triangle.write.format("delta").mode("overwrite").saveAsTable("gold.mart_cumulative_triangle")""",
        "lineage": [
            {"target": "cum_paid_loss", "source": "RETAIN CumPaidLoss + IncrementalPaid", "type": "CUMULATIVE_WINDOW"},
            {"target": "ultimate_estimate", "source": "CumPaidLoss * DevelopmentFactor", "type": "MATH"}
        ],
        "legacy_data": [
            {"accident_year": 2021, "development_year": 0, "incremental_paid": 450000.0, "cum_paid_loss": 450000.0, "ultimate_estimate": 990000.0},
            {"accident_year": 2021, "development_year": 1, "incremental_paid": 220000.0, "cum_paid_loss": 670000.0, "ultimate_estimate": 871000.0},
            {"accident_year": 2021, "development_year": 2, "incremental_paid": 80000.0,  "cum_paid_loss": 750000.0, "ultimate_estimate": 825000.0},
            {"accident_year": 2022, "development_year": 0, "incremental_paid": 510000.0, "cum_paid_loss": 510000.0, "ultimate_estimate": 1122000.0},
            {"accident_year": 2022, "development_year": 1, "incremental_paid": 260000.0, "cum_paid_loss": 770000.0, "ultimate_estimate": 1001000.0},
        ],
        "spark_data": [
            {"accident_year": 2021, "development_year": 0, "incremental_paid": 450000.0, "cum_paid_loss": 450000.0, "ultimate_estimate": 990000.0},
            {"accident_year": 2021, "development_year": 1, "incremental_paid": 220000.0, "cum_paid_loss": 670000.0, "ultimate_estimate": 871000.0},
            {"accident_year": 2021, "development_year": 2, "incremental_paid": 80000.0,  "cum_paid_loss": 750000.0, "ultimate_estimate": 825000.0},
            {"accident_year": 2022, "development_year": 0, "incremental_paid": 510000.0, "cum_paid_loss": 510000.0, "ultimate_estimate": 1122000.0},
            {"accident_year": 2022, "development_year": 1, "incremental_paid": 260000.0, "cum_paid_loss": 770000.0, "ultimate_estimate": 1001000.0},
        ],
    },
    {
        "id": "job_09_broker_commission_tiers",
        "title": "Broker Commission Tier Settlement",
        "dialect": "tsql",
        "tier": "Tier A (Easy - $0 Cost)",
        "tier_code": "A",
        "routing_reason": "Standard multi-table JOIN with scalar arithmetic. Handled deterministically by sqlglot with zero token cost.",
        "layer_in": "Silver (FCT_POLICY_SALES, DIM_BROKER)",
        "layer_out": "Gold (RPT_BROKER_COMMISSIONS)",
        "primary_keys": ["broker_id", "statement_month"],
        "legacy_code": """-- Job: BROKER_COMMISSION_SETTLEMENT
SELECT 
    b.BrokerID AS broker_id,
    b.BrokerName AS broker_name,
    FORMAT(p.SaleDate, 'yyyy-MM') AS statement_month,
    COUNT(p.PolicyID) AS policies_sold,
    ROUND(SUM(p.GrossPremium), 2) AS total_gross_premium,
    ROUND(SUM(p.GrossPremium * b.CommissionRate), 2) AS payable_commission
FROM dbo.FCT_POLICY_SALES p
INNER JOIN dbo.DIM_BROKER b ON p.BrokerID = b.BrokerID
WHERE p.SaleDate >= '2023-01-01'
GROUP BY b.BrokerID, b.BrokerName, FORMAT(p.SaleDate, 'yyyy-MM'), b.CommissionRate;""",
        "pyspark_code": """# PySpark Modernization: Broker Commission Tier Settlement
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("BROKER_COMMISSION_SETTLEMENT").getOrCreate()

df_sales = spark.read.table("silver.fct_policy_sales")
df_broker = spark.read.table("silver.dim_broker")

df_commissions = (
    df_sales.join(df_broker, "BrokerID", "inner")
    .filter(F.col("SaleDate") >= "2023-01-01")
    .groupBy(
        F.col("BrokerID").alias("broker_id"),
        F.col("BrokerName").alias("broker_name"),
        F.date_format("SaleDate", "yyyy-MM").alias("statement_month")
    )
    .agg(
        F.count("PolicyID").alias("policies_sold"),
        F.round(F.sum("GrossPremium"), 2).alias("total_gross_premium"),
        F.round(F.sum(F.col("GrossPremium") * F.col("CommissionRate")), 2).alias("payable_commission")
    )
)

df_commissions.write.format("delta").mode("overwrite").saveAsTable("gold.rpt_broker_commissions")""",
        "lineage": [
            {"target": "broker_id", "source": "b.BrokerID", "type": "DIRECT"},
            {"target": "policies_sold", "source": "COUNT(p.PolicyID)", "type": "AGGREGATE"},
            {"target": "payable_commission", "source": "ROUND(SUM(p.GrossPremium * b.CommissionRate), 2)", "type": "MATH_AGG"}
        ],
        "legacy_data": [
            {"broker_id": "BRK-10", "broker_name": "Apex Insurance Partners", "statement_month": "2023-01", "policies_sold": 85, "total_gross_premium": 85000.0, "payable_commission": 8500.0},
            {"broker_id": "BRK-20", "broker_name": "Celtic Underwriting Ltd", "statement_month": "2023-01", "policies_sold": 120, "total_gross_premium": 144000.0, "payable_commission": 17280.0},
        ],
        "spark_data": [
            {"broker_id": "BRK-10", "broker_name": "Apex Insurance Partners", "statement_month": "2023-01", "policies_sold": 85, "total_gross_premium": 85000.0, "payable_commission": 8500.0},
            {"broker_id": "BRK-20", "broker_name": "Celtic Underwriting Ltd", "statement_month": "2023-01", "policies_sold": 120, "total_gross_premium": 144000.0, "payable_commission": 17280.0},
        ],
    },
    {
        "id": "job_10_actuarial_triangle_pivot",
        "title": "Loss Development Triangular Matrix (PIVOT)",
        "dialect": "tsql",
        "tier": "Tier B (Medium - Hybrid AST + Meta LLM)",
        "tier_code": "B",
        "routing_reason": "Contains T-SQL PIVOT statement over development periods. Transpiled to PySpark .groupBy().pivot().agg().",
        "layer_in": "Silver (FCT_INCREMENTAL_CLAIMS)",
        "layer_out": "Gold (MART_LOSS_DEVELOPMENT_MATRIX)",
        "primary_keys": ["accident_year"],
        "legacy_code": """-- Job: ACTUARIAL_DEVELOPMENT_PIVOT
SELECT 
    AccidentYear AS accident_year,
    ISNULL([0], 0.0) AS dev_year_0,
    ISNULL([1], 0.0) AS dev_year_1,
    ISNULL([2], 0.0) AS dev_year_2
FROM (
    SELECT AccidentYear, DevelopmentYear, PaidAmount 
    FROM dbo.FCT_INCREMENTAL_CLAIMS
) AS SourceTable
PIVOT (
    SUM(PaidAmount)
    FOR DevelopmentYear IN ([0], [1], [2])
) AS PivotTable;""",
        "pyspark_code": """# PySpark Modernization: Loss Development Triangular Matrix (Pivot)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ACTUARIAL_DEVELOPMENT_PIVOT").getOrCreate()

df_matrix = (
    spark.read.table("silver.fct_incremental_claims")
    .groupBy("AccidentYear")
    .pivot("DevelopmentYear", [0, 1, 2])
    .agg(F.coalesce(F.sum("PaidAmount"), F.lit(0.0)))
    .select(
        F.col("AccidentYear").alias("accident_year"),
        F.col("0").alias("dev_year_0"),
        F.col("1").alias("dev_year_1"),
        F.col("2").alias("dev_year_2")
    )
)

df_matrix.write.format("delta").mode("overwrite").saveAsTable("gold.mart_loss_development_matrix")""",
        "lineage": [
            {"target": "accident_year", "source": "AccidentYear", "type": "DIRECT"},
            {"target": "dev_year_0", "source": "PIVOT SUM(PaidAmount) FOR DevYear=0", "type": "PIVOT_AGG"}
        ],
        "legacy_data": [
            {"accident_year": 2021, "dev_year_0": 450000.0, "dev_year_1": 220000.0, "dev_year_2": 80000.0},
            {"accident_year": 2022, "dev_year_0": 510000.0, "dev_year_1": 260000.0, "dev_year_2": 0.0},
        ],
        "spark_data": [
            {"accident_year": 2021, "dev_year_0": 450000.0, "dev_year_1": 220000.0, "dev_year_2": 80000.0},
            {"accident_year": 2022, "dev_year_0": 510000.0, "dev_year_1": 260000.0, "dev_year_2": 0.0},
        ],
    },
    {
        "id": "job_11_ibnr_reserve_recalc",
        "title": "IBNR Actuarial Recalculation (Procedural T-SQL)",
        "dialect": "tsql",
        "tier": "Tier C (Hard - Autonomous Meta Agent Loop)",
        "tier_code": "C",
        "routing_reason": "Contains procedural T-SQL: #temp_reserves, WHILE loop simulation, and dynamic factor recalculation.",
        "layer_in": "Silver (FCT_ACTUARIAL_BASE)",
        "layer_out": "Gold (MART_IBNR_RESERVES)",
        "primary_keys": ["origin_year", "lob"],
        "legacy_code": """-- Job: IBNR_ACTUARIAL_RECALCULATION
DECLARE @InflationFactor FLOAT = 1.055;
SELECT 
    OriginYear,
    LOB,
    ReportedLoss,
    ReportedLoss * @InflationFactor AS TrendedLoss
INTO #TempReserves
FROM dbo.FCT_ACTUARIAL_BASE;

SELECT 
    OriginYear AS origin_year,
    LOB AS lob,
    ROUND(SUM(TrendedLoss * 0.42), 2) AS ibnr_reserve_amount
FROM #TempReserves
GROUP BY OriginYear, LOB;
DROP TABLE #TempReserves;""",
        "pyspark_code": """# PySpark Modernization: IBNR Actuarial Recalculation
# Procedural temporary table refactored into DataFrame pipeline
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("IBNR_ACTUARIAL_RECALCULATION").getOrCreate()

INFLATION_FACTOR = 1.055
IBNR_FACTOR = 0.42

df_ibnr = (
    spark.read.table("silver.fct_actuarial_base")
    .withColumn("trended_loss", F.col("ReportedLoss") * INFLATION_FACTOR)
    .groupBy(F.col("OriginYear").alias("origin_year"), F.col("LOB").alias("lob"))
    .agg(
        F.round(F.sum(F.col("trended_loss") * IBNR_FACTOR), 2).alias("ibnr_reserve_amount")
    )
)

df_ibnr.write.format("delta").mode("overwrite").saveAsTable("gold.mart_ibnr_reserves")""",
        "lineage": [
            {"target": "origin_year", "source": "OriginYear", "type": "DIRECT"},
            {"target": "ibnr_reserve_amount", "source": "ROUND(SUM(TrendedLoss * 0.42), 2)", "type": "MATH_AGG"}
        ],
        "legacy_data": [
            {"origin_year": 2022, "lob": "MOTOR", "ibnr_reserve_amount": 421473.0},
            {"origin_year": 2022, "lob": "PROPERTY", "ibnr_reserve_amount": 298150.0},
        ],
        "spark_data": [
            {"origin_year": 2022, "lob": "MOTOR", "ibnr_reserve_amount": 421473.0},
            {"origin_year": 2022, "lob": "PROPERTY", "ibnr_reserve_amount": 298150.0},
        ],
    },
    {
        "id": "job_12_sas_mortality_experience",
        "title": "Actuarial Mortality Table Scoring (SAS DATA Step)",
        "dialect": "sas",
        "tier": "Tier B (Medium - Hybrid AST + Meta LLM)",
        "tier_code": "B",
        "routing_reason": "Contains SAS DATA step with age group classification and mortality rate scoring.",
        "layer_in": "Silver (LIFE_POLICY_EXPOSURE)",
        "layer_out": "Gold (MART_MORTALITY_EXPERIENCE)",
        "primary_keys": ["age_group", "gender"],
        "legacy_code": """/* SAS Program: MORTALITY_EXPERIENCE_STUDY */
DATA WORK.MORTALITY_SCORED;
    SET INLIB.LIFE_POLICY_EXPOSURE;
    IF AttainedAge < 40 THEN AgeGroup = 'UNDER_40';
    ELSE IF AttainedAge <= 65 THEN AgeGroup = '40_TO_65';
    ELSE AgeGroup = 'OVER_65';
    ExpectedClaims = ExposureUnits * BaseMortalityRate;
RUN;

PROC SUMMARY DATA=WORK.MORTALITY_SCORED NWAY;
    CLASS AgeGroup Gender;
    VAR ExposureUnits ActualDeaths ExpectedClaims;
    OUTPUT OUT=WORK.MORTALITY_EXPERIENCE (DROP=_TYPE_ _FREQ_)
        SUM(ExposureUnits)=TotalExposure
        SUM(ActualDeaths)=TotalActualDeaths
        SUM(ExpectedClaims)=TotalExpectedClaims;
RUN;""",
        "pyspark_code": """# PySpark Modernization: Actuarial Mortality Table Scoring
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MORTALITY_EXPERIENCE_STUDY").getOrCreate()

df_mortality = (
    spark.read.table("silver.life_policy_exposure")
    .withColumn("age_group",
        F.when(F.col("AttainedAge") < 40, "UNDER_40")
         .when(F.col("AttainedAge") <= 65, "40_TO_65")
         .otherwise("OVER_65")
    )
    .withColumn("expected_claims", F.col("ExposureUnits") * F.col("BaseMortalityRate"))
    .groupBy("age_group", F.col("Gender").alias("gender"))
    .agg(
        F.sum("ExposureUnits").alias("total_exposure"),
        F.sum("ActualDeaths").alias("total_actual_deaths"),
        F.round(F.sum("expected_claims"), 2).alias("total_expected_claims")
    )
)

df_mortality.write.format("delta").mode("overwrite").saveAsTable("gold.mart_mortality_experience")""",
        "lineage": [
            {"target": "age_group", "source": "IF AttainedAge < 40...", "type": "CASE_WHEN"},
            {"target": "total_expected_claims", "source": "SUM(ExposureUnits * BaseMortalityRate)", "type": "AGGREGATE"}
        ],
        "legacy_data": [
            {"age_group": "40_TO_65", "gender": "F", "total_exposure": 15000, "total_actual_deaths": 24, "total_expected_claims": 22.8},
            {"age_group": "40_TO_65", "gender": "M", "total_exposure": 14200, "total_actual_deaths": 36, "total_expected_claims": 34.2},
        ],
        "spark_data": [
            {"age_group": "40_TO_65", "gender": "F", "total_exposure": 15000, "total_actual_deaths": 24, "total_expected_claims": 22.8},
            {"age_group": "40_TO_65", "gender": "M", "total_exposure": 14200, "total_actual_deaths": 36, "total_expected_claims": 34.2},
        ],
    }
]


def get_all_jobs() -> list[dict[str, Any]]:
    """Returns the jobs with pre-computed reconciliation scorecards."""
    jobs_with_scores = []
    for j in SAMPLE_JOBS:
        scorecard = reconcile_dataframes(
            legacy_data=j["legacy_data"],
            spark_data=j["spark_data"],
            primary_keys=j.get("primary_keys"),
            job_id=j["id"],
        )
        j_copy = dict(j)
        j_copy["scorecard"] = scorecard.to_dict()
        jobs_with_scores.append(j_copy)
    return jobs_with_scores
