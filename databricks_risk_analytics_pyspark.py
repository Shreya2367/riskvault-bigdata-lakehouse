# =============================================================================
# Risk Analytics in Banking Domain
# Databricks PySpark ETL - Medallion Architecture (Bronze → Silver → Gold)
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, trim, upper,
    round as spark_round, min as spark_min, max as spark_max,
    avg, count, sum as spark_sum
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, LongType
)
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Initialize Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("BankingRiskAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DBFS_INPUT_PATH   = "dbfs:/FileStore/loan_default.csv"
DELTA_ROOT        = "dbfs:/delta/banking_risk"
BRONZE_PATH       = f"{DELTA_ROOT}/bronze/loan_default"
SILVER_PATH       = f"{DELTA_ROOT}/silver/loan_clean"
GOLD_PATH         = f"{DELTA_ROOT}/gold"

DATABASE_NAME     = "banking_risk_analytics"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")


# =============================================================================
# BRONZE LAYER — Raw Ingestion
# =============================================================================
def process_bronze():
    print("=" * 60)
    print("BRONZE LAYER: Ingesting raw CSV data")
    print("=" * 60)

    df_raw = spark.read.csv(
        DBFS_INPUT_PATH,
        header=True,
        inferSchema=True
    )

    df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("source_file", lit(DBFS_INPUT_PATH))

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(BRONZE_PATH)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_loan_default
        USING DELTA LOCATION '{BRONZE_PATH}'
    """)

    count = df_bronze.count()
    print(f"[BRONZE] Records ingested: {count}")
    return df_bronze


# =============================================================================
# SILVER LAYER — Cleaning & Transformation
# =============================================================================
def process_silver():
    print("=" * 60)
    print("SILVER LAYER: Cleaning and transforming data")
    print("=" * 60)

    df = spark.read.format("delta").load(BRONZE_PATH)

    # -- Cast numeric columns --
    numeric_cols = {
        "Age": IntegerType(),
        "Income": DoubleType(),
        "LoanAmount": DoubleType(),
        "CreditScore": IntegerType(),
        "MonthsEmployed": IntegerType(),
        "NumCreditLines": IntegerType(),
        "InterestRate": DoubleType(),
        "LoanTerm": IntegerType(),
        "DTIRatio": DoubleType(),
        "Default": IntegerType(),
    }
    for col_name, dtype in numeric_cols.items():
        df = df.withColumn(col_name, col(col_name).cast(dtype))

    # -- Standardize categorical columns --
    cat_cols = ["Education", "EmploymentType", "MaritalStatus",
                "HasMortgage", "HasDependents", "LoanPurpose", "HasCoSigner"]
    for c in cat_cols:
        df = df.withColumn(c, trim(col(c)))

    # -- Drop nulls --
    df = df.dropna(subset=list(numeric_cols.keys()))

    # -- Remove duplicates --
    df = df.dropDuplicates(["LoanID"])

    # -- Business rule validation --
    df_silver = df.filter(
        (col("Age").between(18, 100)) &
        (col("Income") > 0) &
        (col("LoanAmount") > 0) &
        (col("CreditScore").between(300, 850)) &
        (col("MonthsEmployed") >= 0) &
        (col("InterestRate").between(0, 100)) &
        (col("DTIRatio").between(0, 5)) &
        (col("Default").isin([0, 1]))
    )

    df_silver = df_silver.withColumn("processed_timestamp", current_timestamp())

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_PATH)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.silver_loan_clean
        USING DELTA LOCATION '{SILVER_PATH}'
    """)

    print(f"[SILVER] Clean records: {df_silver.count()}")
    return df_silver


# =============================================================================
# GOLD LAYER — Analytics & Risk Scoring
# =============================================================================
def compute_risk_score(df):
    """
    Risk Score (0–100):
      - Low Credit Score  → 30 pts
      - Low Income        → 20 pts
      - High Loan Amount  → 15 pts
      - High DTI Ratio    → 20 pts
      - High Interest     → 10 pts
      - Low Employment    → 5 pts
    """
    stats = df.agg(
        spark_min("CreditScore").alias("cs_min"), spark_max("CreditScore").alias("cs_max"),
        spark_min("Income").alias("inc_min"),      spark_max("Income").alias("inc_max"),
        spark_min("LoanAmount").alias("la_min"),   spark_max("LoanAmount").alias("la_max"),
        spark_min("DTIRatio").alias("dti_min"),    spark_max("DTIRatio").alias("dti_max"),
        spark_min("InterestRate").alias("ir_min"), spark_max("InterestRate").alias("ir_max"),
        spark_min("MonthsEmployed").alias("me_min"), spark_max("MonthsEmployed").alias("me_max"),
    ).collect()[0]

    def norm(c, mn, mx):
        return (col(c) - lit(mn)) / lit(max(mx - mn, 1e-9))

    cs_risk  = (lit(1) - norm("CreditScore",   stats["cs_min"],  stats["cs_max"]))  * lit(30)
    inc_risk = (lit(1) - norm("Income",         stats["inc_min"], stats["inc_max"])) * lit(20)
    la_risk  = norm("LoanAmount",  stats["la_min"],  stats["la_max"])  * lit(15)
    dti_risk = norm("DTIRatio",    stats["dti_min"], stats["dti_max"]) * lit(20)
    ir_risk  = norm("InterestRate",stats["ir_min"],  stats["ir_max"])  * lit(10)
    me_risk  = (lit(1) - norm("MonthsEmployed", stats["me_min"], stats["me_max"])) * lit(5)

    df = df.withColumn("RiskScore", spark_round(cs_risk + inc_risk + la_risk + dti_risk + ir_risk + me_risk, 2))

    df = df.withColumn(
        "RiskCategory",
        when(col("RiskScore") <= 40, "Low Risk")
        .when(col("RiskScore") <= 70, "Medium Risk")
        .otherwise("High Risk")
    )
    return df


def process_gold():
    print("=" * 60)
    print("GOLD LAYER: Creating analytics tables")
    print("=" * 60)

    df = spark.read.format("delta").load(SILVER_PATH)
    df = compute_risk_score(df)

    # ------------------------------------------------------------------
    # 1. customer_risk_score — full scored dataset
    # ------------------------------------------------------------------
    path = f"{GOLD_PATH}/customer_risk_score"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_customer_risk_score USING DELTA LOCATION '{path}'")
    print("[GOLD] customer_risk_score created")

    # ------------------------------------------------------------------
    # 2. kpi_summary
    # ------------------------------------------------------------------
    kpi = df.agg(
        count("LoanID").alias("total_customers"),
        spark_sum("Default").alias("total_defaults"),
        spark_round(spark_sum("Default") / count("LoanID") * 100, 2).alias("default_rate_pct"),
        spark_round(avg("LoanAmount"), 2).alias("avg_loan_amount"),
        spark_round(avg("CreditScore"), 2).alias("avg_credit_score"),
        spark_round(avg("InterestRate"), 2).alias("avg_interest_rate"),
        spark_round(avg("RiskScore"), 2).alias("avg_risk_score"),
        spark_sum(when(col("RiskCategory") == "High Risk", 1).otherwise(0)).alias("high_risk_count"),
        spark_sum(when(col("RiskCategory") == "Medium Risk", 1).otherwise(0)).alias("medium_risk_count"),
        spark_sum(when(col("RiskCategory") == "Low Risk", 1).otherwise(0)).alias("low_risk_count"),
    )
    path = f"{GOLD_PATH}/kpi_summary"
    kpi.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_kpi_summary USING DELTA LOCATION '{path}'")
    print("[GOLD] kpi_summary created")

    # ------------------------------------------------------------------
    # 3. loan_default_summary
    # ------------------------------------------------------------------
    ld = df.groupBy("LoanPurpose").agg(
        count("LoanID").alias("total_loans"),
        spark_sum("Default").alias("total_defaults"),
        spark_round(spark_sum("Default") / count("LoanID") * 100, 2).alias("default_rate_pct"),
        spark_round(avg("LoanAmount"), 2).alias("avg_loan_amount"),
        spark_round(avg("InterestRate"), 2).alias("avg_interest_rate"),
    )
    path = f"{GOLD_PATH}/loan_default_summary"
    ld.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_loan_default_summary USING DELTA LOCATION '{path}'")
    print("[GOLD] loan_default_summary created")

    # ------------------------------------------------------------------
    # 4. loan_purpose_risk
    # ------------------------------------------------------------------
    lpr = df.groupBy("LoanPurpose", "RiskCategory").agg(
        count("LoanID").alias("customer_count"),
        spark_round(avg("RiskScore"), 2).alias("avg_risk_score"),
        spark_sum("Default").alias("defaults"),
    )
    path = f"{GOLD_PATH}/loan_purpose_risk"
    lpr.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_loan_purpose_risk USING DELTA LOCATION '{path}'")
    print("[GOLD] loan_purpose_risk created")

    # ------------------------------------------------------------------
    # 5. employment_risk_summary
    # ------------------------------------------------------------------
    ers = df.groupBy("EmploymentType").agg(
        count("LoanID").alias("total_customers"),
        spark_sum("Default").alias("total_defaults"),
        spark_round(spark_sum("Default") / count("LoanID") * 100, 2).alias("default_rate_pct"),
        spark_round(avg("RiskScore"), 2).alias("avg_risk_score"),
        spark_round(avg("Income"), 2).alias("avg_income"),
    )
    path = f"{GOLD_PATH}/employment_risk_summary"
    ers.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_employment_risk_summary USING DELTA LOCATION '{path}'")
    print("[GOLD] employment_risk_summary created")

    # ------------------------------------------------------------------
    # 6. credit_score_risk_summary
    # ------------------------------------------------------------------
    df_cs = df.withColumn(
        "CreditScoreRange",
        when(col("CreditScore") < 500, "300-499")
        .when(col("CreditScore") < 600, "500-599")
        .when(col("CreditScore") < 700, "600-699")
        .when(col("CreditScore") < 800, "700-799")
        .otherwise("800-850")
    )
    csr = df_cs.groupBy("CreditScoreRange", "RiskCategory").agg(
        count("LoanID").alias("customer_count"),
        spark_sum("Default").alias("total_defaults"),
        spark_round(avg("RiskScore"), 2).alias("avg_risk_score"),
    )
    path = f"{GOLD_PATH}/credit_score_risk_summary"
    csr.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_credit_score_risk_summary USING DELTA LOCATION '{path}'")
    print("[GOLD] credit_score_risk_summary created")

    # ------------------------------------------------------------------
    # 7. financial_risk_summary
    # ------------------------------------------------------------------
    frs = df.groupBy("RiskCategory").agg(
        spark_round(avg("LoanAmount"), 2).alias("avg_loan_amount"),
        spark_round(avg("InterestRate"), 2).alias("avg_interest_rate"),
        spark_round(avg("DTIRatio"), 4).alias("avg_dti_ratio"),
        spark_round(avg("Income"), 2).alias("avg_income"),
        count("LoanID").alias("customer_count"),
        spark_sum("Default").alias("total_defaults"),
        spark_round(spark_sum("Default") / count("LoanID") * 100, 2).alias("default_rate_pct"),
    )
    path = f"{GOLD_PATH}/financial_risk_summary"
    frs.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_financial_risk_summary USING DELTA LOCATION '{path}'")
    print("[GOLD] financial_risk_summary created")

    print("[GOLD] All Gold tables created successfully!")
    return df


# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":
    print("\n" + "="*60)
    print("  BANKING RISK ANALYTICS — MEDALLION ETL PIPELINE")
    print("="*60 + "\n")

    process_bronze()
    process_silver()
    process_gold()

    print("\n[PIPELINE COMPLETE] All layers processed successfully.")
    print(f"Database: {DATABASE_NAME}")
    print(f"Delta Root: {DELTA_ROOT}")
    spark.stop()
