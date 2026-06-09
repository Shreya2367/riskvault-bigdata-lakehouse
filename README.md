# Risk Analytics in Banking Domain
### End-to-End Big Data Pipeline using Airflow · Databricks · PySpark · MySQL · Power BI

---

## Project Overview

This is an industry-level **Banking Risk Analytics** project that demonstrates a complete, production-style Big Data pipeline for **Loan Default Prediction** and **Customer Risk Scoring**. Built on the **Medallion Architecture** with **Delta Lake**, orchestrated by **Apache Airflow**, processed by **PySpark on Databricks**, stored in **MySQL**, and visualized in a four-page interactive dashboard.

The project processes **20,099 loan applications** across 18 dimensions, applies a proprietary risk-scoring algorithm, and produces actionable insights for banking risk teams.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Big Data Processing | PySpark 3.5 on Databricks |
| Storage Format | Delta Lake |
| Architecture Pattern | Medallion (Bronze / Silver / Gold) |
| Analytics Database | MySQL 8.x |
| Reporting | Power BI / Interactive HTML |
| Language | Python 3.11 |

---

## Architecture

```
Loan Default CSV
      ↓
Apache Airflow DAG (Daily @ 02:00 UTC)
      ↓
Databricks Cluster (PySpark)
      ↓
┌─────────────────────────────────────────┐
│        MEDALLION ARCHITECTURE           │
│                                         │
│  Bronze Layer  →  Raw Delta table       │
│       ↓                                 │
│  Silver Layer  →  Clean Delta table     │
│       ↓                                 │
│  Gold Layer    →  7 Analytics tables    │
└─────────────────────────────────────────┘
      ↓
MySQL (7 analytics tables)
      ↓
Power BI / Interactive HTML Dashboard
```

---

## Medallion Architecture

### Bronze Layer
- Reads raw CSV from DBFS
- Adds `ingestion_timestamp` and `source_file` metadata
- Stores as Delta table `bronze_loan_default` (schema preserved)

### Silver Layer
- Drops null values across all key numeric columns
- Removes duplicate `LoanID` records
- Casts columns to proper data types (Int, Double, String)
- Standardizes categorical columns (trim whitespace)
- Applies business validation rules:
  - Age: 18–100
  - CreditScore: 300–850
  - Income > 0, LoanAmount > 0
  - DTIRatio: 0–5
  - Default: 0 or 1

### Gold Layer
Creates 7 analytics Delta tables:

| Table | Description |
|---|---|
| `gold_customer_risk_score` | Full dataset with risk scores |
| `gold_kpi_summary` | Daily business KPIs |
| `gold_loan_default_summary` | Default analytics by loan purpose |
| `gold_loan_purpose_risk` | Risk distribution by purpose |
| `gold_employment_risk_summary` | Risk by employment type |
| `gold_credit_score_risk_summary` | Credit score range vs risk |
| `gold_financial_risk_summary` | Financial metrics by risk category |

---

## Risk Score Methodology

**Score Range: 0–100** (Higher = More Risk)

| Factor | Weight | Logic |
|---|---|---|
| Credit Score | 30% | Lower score → higher risk |
| Income | 20% | Lower income → higher risk |
| Loan Amount | 15% | Higher loan → higher risk |
| DTI Ratio | 20% | Higher DTI → higher risk |
| Interest Rate | 10% | Higher rate → higher risk |
| Months Employed | 5% | Less tenure → higher risk |

**Risk Categories:**
- `Low Risk`: Score 0–40 (3,492 customers)
- `Medium Risk`: Score 41–70 (14,132 customers)
- `High Risk`: Score 71–100 (2,475 customers)

---

## Airflow Workflow

DAG ID: `banking_risk_analytics_pipeline`
Schedule: `0 2 * * *` (Daily at 02:00 UTC)

```
start_pipeline
    → ingest_bronze_databricks → ingest_bronze_data
    → process_silver_databricks → process_silver_data
    → create_gold_databricks → create_gold_tables
    → export_to_mysql
    → generate_dashboard_data
    → end_pipeline
```

Features:
- Retries: 3 with exponential backoff
- XCom-based status tracking across tasks
- Email alerts on failure
- Max 1 active DAG run (prevents duplicate processing)

---

## Databricks Processing

The PySpark notebook (`databricks_risk_analytics_pyspark.py`) runs on Databricks and:

1. Creates database `banking_risk_analytics`
2. Reads raw CSV → Bronze Delta table
3. Cleans and validates → Silver Delta table
4. Computes risk scores using normalized weighted formula
5. Creates 7 Gold Delta analytics tables
6. All tables stored in `dbfs:/delta/banking_risk/`

---

## MySQL Integration

**Database:** `banking_risk_analytics`

Tables created and populated:
1. `kpi_summary` — daily KPI snapshot
2. `customer_risk_score` — full scored customer data
3. `loan_default_analytics` — default rates by loan purpose
4. `loan_purpose_risk` — risk distribution by purpose
5. `employment_risk_summary` — employment type risk
6. `credit_score_risk_summary` — credit range vs risk
7. `financial_risk_summary` — financial metrics by risk category

---

## Dashboard

Four-page interactive dashboard (`Risk_Analytics_Banking_Dashboard.html`):

| Page | Content |
|---|---|
| Page 1 — Executive Summary | 9 KPI cards, risk distribution, default overview |
| Page 2 — Loan Default Analytics | Default by purpose, employment, loan amount, interest rate |
| Page 3 — Customer Risk Score | Top 10 high-risk customers, age/credit vs risk |
| Page 4 — Financial Risk Insights | Loan/rate/education/marital/mortgage analytics |

---

## Project Folder Structure

```
Risk_Analytics_Banking_Project/
│
├── data/
│   └── loan_default_processed_with_risk_score.csv
│
├── databricks/
│   └── databricks_risk_analytics_pyspark.py
│
├── airflow/
│   └── airflow_risk_analytics_dag.py
│
├── mysql/
│   ├── mysql_create_tables.sql
│   └── mysql_insert_final_tables.sql
│
├── dashboard/
│   └── Risk_Analytics_Banking_Dashboard.html
│
├── docs/
│   ├── README.md
│   ├── project_report.md
│   └── project_architecture_flowchart.txt
│
└── requirements.txt
```

---

## Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Airflow
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@bank.com --firstname Admin --lastname User
# Add connections: databricks_default, mysql_banking_risk
airflow dags trigger banking_risk_analytics_pipeline
```

### 3. Configure Databricks
- Upload CSV to `dbfs:/FileStore/loan_default.csv`
- Upload `databricks_risk_analytics_pyspark.py` to your Databricks workspace
- Configure cluster (Spark 3.5, Delta Lake enabled)

### 4. Set Up MySQL
```bash
mysql -u root -p < mysql/mysql_create_tables.sql
mysql -u root -p < mysql/mysql_insert_final_tables.sql
```

### 5. View Dashboard
Open `dashboard/Risk_Analytics_Banking_Dashboard.html` in any browser.

---

## Key Metrics (Dataset Summary)

| Metric | Value |
|---|---|
| Total Loan Applications | 20,099 |
| Total Defaults | 2,322 |
| Default Rate | 11.55% |
| Average Loan Amount | $127,572 |
| Average Credit Score | 576 |
| Average Interest Rate | 13.49% |
| High Risk Customers | 2,475 |
| Medium Risk Customers | 14,132 |
| Low Risk Customers | 3,492 |
| Average Risk Score | 53.53 |

---

## Resume Description

> **Risk Analytics in Banking Domain** | Airflow · Databricks · PySpark · Delta Lake · MySQL · Power BI
>
> Built an end-to-end banking risk analytics pipeline processing 20,099 loan applications using Apache Airflow for orchestration, PySpark on Databricks for distributed ETL, and Delta Lake Medallion Architecture (Bronze/Silver/Gold). Developed a weighted risk-scoring engine producing customer risk scores (0–100) and categories. Exported analytics to MySQL and delivered a four-page interactive dashboard with 9 KPIs, default analytics, and financial risk insights.

---

*Generated for Data Engineering / Big Data / Banking Analytics Portfolio*
