# Project Report: Risk Analytics in Banking Domain

**Project:** End-to-End Big Data Banking Risk Analytics Pipeline  
**Tech Stack:** Apache Airflow · Databricks · PySpark · Delta Lake · MySQL · Power BI  
**Dataset:** Loan Default Dataset (20,099 records, 18 features)

---

## Abstract

This project presents a production-grade, end-to-end **Banking Risk Analytics** system built using modern Big Data technologies. The system ingests raw loan application data, processes it through a **Medallion Architecture** on **Delta Lake** via **Apache Spark** on **Databricks**, orchestrates the pipeline with **Apache Airflow**, stores analytics results in **MySQL**, and delivers insights through a four-page interactive dashboard. A proprietary **Risk Scoring Engine** assigns each customer a risk score (0–100) and categorizes them as Low, Medium, or High Risk — enabling proactive loan default prevention and financial risk management.

---

## Problem Statement

Banking institutions face significant financial losses from loan defaults. With growing loan portfolios and diverse customer profiles, manually assessing risk is impractical. There is a critical need for:

- Automated, scalable risk assessment pipelines
- Data-driven customer risk categorization
- Real-time monitoring of portfolio health KPIs
- Actionable dashboards for risk officers and executives

This project addresses these challenges by building an automated Big Data pipeline that processes the entire loan portfolio daily, computes individual customer risk scores, and surfaces insights through interactive dashboards.

---

## Objectives

1. Build a scalable batch ETL pipeline using PySpark on Databricks
2. Implement Delta Lake Medallion Architecture (Bronze → Silver → Gold)
3. Develop a weighted Risk Scoring Engine (0–100 scale)
4. Orchestrate daily pipeline execution via Apache Airflow
5. Export analytics data to MySQL for BI consumption
6. Deliver a four-page interactive dashboard with banking KPIs
7. Create a portfolio-ready, resume-quality project implementation

---

## Architecture

The system follows a layered architecture:

**Layer 1 — Data Source:** Raw Loan Default CSV (20,099 records × 18 columns)

**Layer 2 — Orchestration:** Apache Airflow DAG (`banking_risk_analytics_pipeline`) with 9 tasks, daily schedule, retry logic, and XCom-based status passing.

**Layer 3 — Processing:** Databricks cluster running PySpark, implementing the full Medallion Architecture with Delta Lake for ACID-compliant storage.

**Layer 4 — Storage:** MySQL database (`banking_risk_analytics`) with 7 analytics tables for BI consumption.

**Layer 5 — Reporting:** Interactive HTML dashboard (or Power BI) with four analytical pages.

---

## ETL Workflow

### Bronze Layer (Raw Ingestion)
- Source: `loan_default.csv` uploaded to DBFS
- Operation: Schema-preserving read, metadata enrichment (timestamp, source path)
- Output: `bronze_loan_default` Delta table
- Record Count: 20,099 raw records

### Silver Layer (Cleaning & Validation)
- **Null Removal:** Drop records with nulls in Age, Income, LoanAmount, CreditScore, MonthsEmployed, InterestRate, DTIRatio, Default
- **Deduplication:** Remove duplicate LoanID records
- **Type Casting:** Age/CreditScore/MonthsEmployed → Integer; Income/LoanAmount/InterestRate/DTIRatio → Double
- **Categorical Standardization:** Trim whitespace from all string columns
- **Business Rules Validation:**
  - Age between 18 and 100
  - Income and LoanAmount > 0
  - CreditScore between 300 and 850
  - DTIRatio between 0 and 5
  - Default values restricted to 0 or 1
- Output: `silver_loan_clean` Delta table

### Gold Layer (Analytics)
- Risk Score computation using 6-factor weighted formula
- Risk Category assignment (Low/Medium/High)
- 7 analytics aggregation tables created as Delta tables
- All tables stored in `dbfs:/delta/banking_risk/gold/`

---

## Databricks Transformations

### Risk Scoring Engine

All six input features are Min-Max normalized to [0, 1], then weighted:

```
RiskScore = (1 - CreditScore_norm) × 30
          + (1 - Income_norm)       × 20
          + LoanAmount_norm         × 15
          + DTIRatio_norm           × 20
          + InterestRate_norm       × 10
          + (1 - MonthsEmployed_norm) × 5
```

Final score is scaled to [0, 100]. Higher score = higher default risk.

### Gold Tables Created

| Table | Granularity | Key Metrics |
|---|---|---|
| customer_risk_score | Per customer | RiskScore, RiskCategory |
| kpi_summary | Daily aggregate | DefaultRate, AvgLoanAmount, HighRiskCount |
| loan_default_summary | By LoanPurpose | DefaultRate, TotalLoans |
| loan_purpose_risk | Purpose × RiskCategory | CustomerCount, AvgRiskScore |
| employment_risk_summary | By EmploymentType | DefaultRate, AvgIncome |
| credit_score_risk_summary | CreditRange × RiskCategory | CustomerCount, TotalDefaults |
| financial_risk_summary | By RiskCategory | AvgLoanAmount, AvgInterestRate, DTIRatio |

---

## Airflow Orchestration

**DAG ID:** `banking_risk_analytics_pipeline`  
**Schedule:** `0 2 * * *` (Daily at 02:00 UTC)  
**Max Active Runs:** 1

### Task Dependency Chain:
```
start_pipeline
→ ingest_bronze_databricks → ingest_bronze_data (validation)
→ process_silver_databricks → process_silver_data (validation)
→ create_gold_databricks → create_gold_tables (validation)
→ export_to_mysql
→ generate_dashboard_data
→ end_pipeline
```

### Reliability Features:
- 3 retries with exponential backoff (max 30 min delay)
- 2-hour execution timeout per task
- XCom-based inter-task status passing
- Email alerts on failure
- Databricks `SubmitRunOperator` for notebook execution

---

## Risk Score Methodology

### Factor Justification

| Factor | Weight | Rationale |
|---|---|---|
| CreditScore | 30% | Primary predictor of repayment behavior |
| DTIRatio | 20% | Debt burden directly predicts default |
| Income | 20% | Repayment capacity fundamental metric |
| LoanAmount | 15% | Larger loans carry higher absolute risk |
| InterestRate | 10% | Higher rates signal riskier borrowers |
| MonthsEmployed | 5% | Employment stability secondary indicator |

### Score Distribution (Dataset)
- Low Risk (0–40): 3,492 customers (17.4%)
- Medium Risk (41–70): 14,132 customers (70.3%)
- High Risk (71–100): 2,475 customers (12.3%)
- Mean Score: 53.53 | Std Dev: 13.89

---

## Dashboard Analytics

### Page 1 — Executive Summary
Nine KPI cards providing instant portfolio health snapshot: Total Customers (20,099), Total Defaults (2,322), Default Rate (11.55%), Avg Loan Amount ($127,572), Avg Credit Score (576), Avg Interest Rate (13.49%), High Risk Count (2,475), Avg Risk Score (53.53).

### Page 2 — Loan Default Analytics
Default rates by loan purpose: Business (12.45%), Other (11.95%), Education (11.56%), Auto (11.01%), Home (10.78%). Employment type and interest rate impact visualized.

### Page 3 — Customer Risk Score
Risk distribution charts, top 10 highest-risk customers table, age group and credit score range heatmaps showing risk concentration patterns.

### Page 4 — Financial Risk Insights
High Risk customers carry 79% higher average loan amounts ($167,501 vs $93,591 for Low Risk), 41% higher interest rates (15.91% vs 11.26%), and 49% lower incomes ($53,156 vs $108,014).

---

## Key Performance Indicators (KPIs)

| KPI | Value |
|---|---|
| Portfolio Size | 20,099 loan applications |
| Default Count | 2,322 |
| Default Rate | 11.55% |
| Avg Loan Amount | $127,572 |
| Avg Credit Score | 576 / 850 |
| Avg Interest Rate | 13.49% |
| Avg DTI Ratio | ~0.50 |
| High Risk Customers | 2,475 (12.3%) |
| Highest Default Rate (Purpose) | Business (12.45%) |
| Lowest Default Rate (Purpose) | Home (10.78%) |

---

## Business Impact

1. **Risk Prevention:** Identifying 2,475 High Risk customers enables proactive intervention before default
2. **Portfolio Health Monitoring:** Daily KPI refresh ensures risk officers have current portfolio view
3. **Loan Pricing:** Risk-based interest rate justification (High Risk avg: 15.91% vs Low Risk: 11.26%)
4. **Credit Policy:** Data-driven credit score thresholds for loan approval
5. **Regulatory Compliance:** Audit-ready Delta Lake tables with full lineage
6. **Cost Reduction:** Automated pipeline replaces manual risk assessment workflows

---

## Conclusion

This project successfully demonstrates a complete, production-quality Big Data Banking Risk Analytics system. The Medallion Architecture ensures data quality at each processing stage. The six-factor Risk Scoring Engine provides interpretable, weighted customer risk assessments. The Airflow DAG enables reliable daily automation with comprehensive error handling. The interactive dashboard delivers executive-level insights and granular drill-down capability.

The project is portfolio-ready for Data Engineering, Big Data Engineering, and Banking Analytics roles.

---

## Future Enhancements

1. **ML Integration:** Replace weighted formula with XGBoost/Random Forest trained model
2. **Real-time Streaming:** Add Kafka + Spark Streaming for real-time loan application scoring
3. **Feature Store:** Integrate Databricks Feature Store for reusable feature engineering
4. **Data Quality:** Implement Great Expectations for automated data validation
5. **Model Monitoring:** Track risk score distribution drift over time
6. **Multi-bank:** Extend to multi-tenant architecture for banking group portfolios
7. **Explainability:** Add SHAP values for individual risk score explanation
8. **Alerting:** Slack/Teams integration for high-risk customer alerts via Airflow

---

*Report generated for: Data Engineering Portfolio | Banking Analytics Domain*
