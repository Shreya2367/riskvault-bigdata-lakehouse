-- =============================================================================
-- MySQL: Banking Risk Analytics — Table Creation Script
-- Project: Risk Analytics in Banking Domain
-- =============================================================================

CREATE DATABASE IF NOT EXISTS banking_risk_analytics
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE banking_risk_analytics;

-- =============================================================================
-- TABLE 1: kpi_summary
-- =============================================================================
DROP TABLE IF EXISTS kpi_summary;
CREATE TABLE kpi_summary (
    id                  INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_date            DATE NOT NULL,
    total_customers     INT UNSIGNED NOT NULL DEFAULT 0,
    total_defaults      INT UNSIGNED NOT NULL DEFAULT 0,
    default_rate_pct    DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    avg_loan_amount     DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    avg_credit_score    DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    avg_interest_rate   DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    avg_risk_score      DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    high_risk_count     INT UNSIGNED NOT NULL DEFAULT 0,
    medium_risk_count   INT UNSIGNED NOT NULL DEFAULT 0,
    low_risk_count      INT UNSIGNED NOT NULL DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_run_date  (run_date)
) ENGINE=InnoDB COMMENT='Daily KPI summary for banking risk analytics';

-- =============================================================================
-- TABLE 2: customer_risk_score
-- =============================================================================
DROP TABLE IF EXISTS customer_risk_score;
CREATE TABLE customer_risk_score (
    id                  BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    loan_id             VARCHAR(20) NOT NULL,
    age                 TINYINT UNSIGNED,
    income              DECIMAL(12,2),
    loan_amount         DECIMAL(12,2),
    credit_score        SMALLINT UNSIGNED,
    months_employed     SMALLINT UNSIGNED,
    num_credit_lines    TINYINT UNSIGNED,
    interest_rate       DECIMAL(5,2),
    loan_term           SMALLINT UNSIGNED,
    dti_ratio           DECIMAL(6,4),
    education           VARCHAR(50),
    employment_type     VARCHAR(50),
    marital_status      VARCHAR(30),
    has_mortgage        VARCHAR(5),
    has_dependents      VARCHAR(5),
    loan_purpose        VARCHAR(50),
    has_cosigner        VARCHAR(5),
    default_flag        TINYINT(1) NOT NULL DEFAULT 0,
    risk_score          DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    risk_category       ENUM('Low Risk','Medium Risk','High Risk') NOT NULL,
    processed_date      DATE NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_loan_id_date (loan_id, processed_date),
    INDEX idx_risk_category (risk_category),
    INDEX idx_credit_score  (credit_score),
    INDEX idx_default_flag  (default_flag),
    INDEX idx_processed_date (processed_date)
) ENGINE=InnoDB COMMENT='Customer-level risk scores and categories';

-- =============================================================================
-- TABLE 3: loan_default_analytics
-- =============================================================================
DROP TABLE IF EXISTS loan_default_analytics;
CREATE TABLE loan_default_analytics (
    id                  INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_date            DATE NOT NULL,
    loan_purpose        VARCHAR(50) NOT NULL,
    total_loans         INT UNSIGNED NOT NULL DEFAULT 0,
    total_defaults      INT UNSIGNED NOT NULL DEFAULT 0,
    default_rate_pct    DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    avg_loan_amount     DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    avg_interest_rate   DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_purpose (run_date, loan_purpose),
    INDEX idx_loan_purpose  (loan_purpose),
    INDEX idx_default_rate  (default_rate_pct)
) ENGINE=InnoDB COMMENT='Loan default analytics by loan purpose';

-- =============================================================================
-- TABLE 4: loan_purpose_risk
-- =============================================================================
DROP TABLE IF EXISTS loan_purpose_risk;
CREATE TABLE loan_purpose_risk (
    id                  INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_date            DATE NOT NULL,
    loan_purpose        VARCHAR(50) NOT NULL,
    risk_category       ENUM('Low Risk','Medium Risk','High Risk') NOT NULL,
    customer_count      INT UNSIGNED NOT NULL DEFAULT 0,
    avg_risk_score      DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    defaults            INT UNSIGNED NOT NULL DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_purpose_risk (run_date, loan_purpose, risk_category),
    INDEX idx_loan_purpose  (loan_purpose),
    INDEX idx_risk_category (risk_category)
) ENGINE=InnoDB COMMENT='Risk distribution by loan purpose';

-- =============================================================================
-- TABLE 5: employment_risk_summary
-- =============================================================================
DROP TABLE IF EXISTS employment_risk_summary;
CREATE TABLE employment_risk_summary (
    id                  INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_date            DATE NOT NULL,
    employment_type     VARCHAR(50) NOT NULL,
    total_customers     INT UNSIGNED NOT NULL DEFAULT 0,
    total_defaults      INT UNSIGNED NOT NULL DEFAULT 0,
    default_rate_pct    DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    avg_risk_score      DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    avg_income          DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_emp (run_date, employment_type),
    INDEX idx_employment_type (employment_type)
) ENGINE=InnoDB COMMENT='Risk analytics by employment type';

-- =============================================================================
-- TABLE 6: credit_score_risk_summary
-- =============================================================================
DROP TABLE IF EXISTS credit_score_risk_summary;
CREATE TABLE credit_score_risk_summary (
    id                     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_date               DATE NOT NULL,
    credit_score_range     VARCHAR(20) NOT NULL,
    risk_category          ENUM('Low Risk','Medium Risk','High Risk') NOT NULL,
    customer_count         INT UNSIGNED NOT NULL DEFAULT 0,
    total_defaults         INT UNSIGNED NOT NULL DEFAULT 0,
    avg_risk_score         DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_cs_risk (run_date, credit_score_range, risk_category),
    INDEX idx_credit_range  (credit_score_range),
    INDEX idx_risk_category (risk_category)
) ENGINE=InnoDB COMMENT='Credit score range vs risk category analysis';

-- =============================================================================
-- TABLE 7: financial_risk_summary
-- =============================================================================
DROP TABLE IF EXISTS financial_risk_summary;
CREATE TABLE financial_risk_summary (
    id                  INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_date            DATE NOT NULL,
    risk_category       ENUM('Low Risk','Medium Risk','High Risk') NOT NULL,
    avg_loan_amount     DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    avg_interest_rate   DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    avg_dti_ratio       DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    avg_income          DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    customer_count      INT UNSIGNED NOT NULL DEFAULT 0,
    total_defaults      INT UNSIGNED NOT NULL DEFAULT 0,
    default_rate_pct    DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_risk (run_date, risk_category),
    INDEX idx_risk_category (risk_category)
) ENGINE=InnoDB COMMENT='Financial metrics by risk category';

-- Verify tables
SELECT
    TABLE_NAME,
    ENGINE,
    TABLE_ROWS,
    TABLE_COMMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'banking_risk_analytics'
ORDER BY TABLE_NAME;
