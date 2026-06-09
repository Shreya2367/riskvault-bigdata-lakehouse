-- =============================================================================
-- MySQL: Banking Risk Analytics — Insert / Export Script
-- Generated: 2025-01-15
-- =============================================================================

USE banking_risk_analytics;

-- KPI Summary
INSERT INTO kpi_summary (run_date, total_customers, total_defaults, default_rate_pct, avg_loan_amount, avg_credit_score, avg_interest_rate, avg_risk_score, high_risk_count, medium_risk_count, low_risk_count)
VALUES ('2025-01-15', 20099, 2322, 11.55, 127571.67, 575.88, 13.49, 53.53, 2475, 14132, 3492);

-- Loan Default Analytics
INSERT INTO loan_default_analytics (run_date, loan_purpose, total_loans, total_defaults, default_rate_pct, avg_loan_amount, avg_interest_rate)
VALUES
  ('2025-01-15', 'Auto', 4007, 441, 11.01, 0.00, 0.00),
  ('2025-01-15', 'Business', 4040, 503, 12.45, 0.00, 0.00),
  ('2025-01-15', 'Education', 4039, 467, 11.56, 0.00, 0.00),
  ('2025-01-15', 'Home', 3953, 426, 10.78, 0.00, 0.00),
  ('2025-01-15', 'Other', 4060, 485, 11.95, 0.00, 0.00);

-- Employment Risk Summary
INSERT INTO employment_risk_summary (run_date, employment_type, total_customers, total_defaults, default_rate_pct, avg_risk_score, avg_income)
VALUES
  ('2025-01-15', 'Full-time', 5025, 487, 9.69, 0.00, 0.00),
  ('2025-01-15', 'Part-time', 4990, 615, 12.32, 0.00, 0.00),
  ('2025-01-15', 'Self-employed', 5137, 543, 10.57, 0.00, 0.00),
  ('2025-01-15', 'Unemployed', 4947, 677, 13.69, 0.00, 0.00);

-- Financial Risk Summary
INSERT INTO financial_risk_summary (run_date, risk_category, avg_loan_amount, avg_interest_rate, avg_dti_ratio, avg_income, customer_count, total_defaults, default_rate_pct)
VALUES
  ('2025-01-15', 'High Risk', 167501.29, 15.91, 0.0000, 53156.0, 2475, 0, 0.00),
  ('2025-01-15', 'Low Risk', 93591.0, 11.26, 0.0000, 108013.87, 3492, 0, 0.00),
  ('2025-01-15', 'Medium Risk', 128975.2, 13.61, 0.0000, 81661.38, 14132, 0, 0.00);

-- Customer Risk Score (sample 50 rows shown; full dataset loaded via ETL)
INSERT INTO customer_risk_score (loan_id, age, income, loan_amount, credit_score, months_employed, num_credit_lines, interest_rate, loan_term, dti_ratio, education, employment_type, marital_status, has_mortgage, has_dependents, loan_purpose, has_cosigner, default_flag, risk_score, risk_category, processed_date)
VALUES
  ('I38PQUQS96', 56, 85994, 50587, 520, 80, 4, 15.23, 36, 0.44, 'Bachelor's', 'Full-time', 'Divorced', 'Yes', 'Yes', 'Other', 'Yes', 0, 49.42, 'Medium Risk', '2025-01-15'),
  ('HPSK72WA7R', 69, 50432, 124440, 458, 15, 1, 4.81, 60, 0.68, 'Master's', 'Full-time', 'Married', 'No', 'No', 'Other', 'Yes', 0, 68.03, 'Medium Risk', '2025-01-15'),
  ('C1OZ6DPJ8Y', 46, 84208, 129188, 451, 26, 3, 21.17, 24, 0.31, 'Master's', 'Unemployed', 'Divorced', 'Yes', 'Yes', 'Auto', 'No', 1, 60.61, 'Medium Risk', '2025-01-15'),
  ('V2KKSFM3UN', 32, 31713, 44799, 743, 0, 3, 7.07, 24, 0.23, 'High School', 'Full-time', 'Married', 'No', 'No', 'Business', 'No', 0, 38.78, 'Low Risk', '2025-01-15'),
  ('EY08JDHTZP', 60, 20437, 9139, 633, 8, 4, 6.51, 48, 0.73, 'Bachelor's', 'Unemployed', 'Divorced', 'No', 'Yes', 'Auto', 'No', 0, 57.44, 'Medium Risk', '2025-01-15'),
  ('A9S62RQ7US', 25, 90298, 90448, 720, 18, 2, 22.72, 24, 0.1, 'High School', 'Unemployed', 'Single', 'Yes', 'No', 'Business', 'Yes', 1, 36.82, 'Low Risk', '2025-01-15'),
  ('H8GXPAOS71', 38, 111188, 177025, 429, 80, 1, 19.11, 12, 0.16, 'Bachelor's', 'Unemployed', 'Single', 'Yes', 'No', 'Home', 'Yes', 0, 53.35, 'Medium Risk', '2025-01-15'),
  ('0HGZQKJ36W', 56, 126802, 155511, 531, 67, 4, 8.15, 60, 0.43, 'PhD', 'Full-time', 'Married', 'No', 'No', 'Home', 'Yes', 0, 46.2, 'Medium Risk', '2025-01-15'),
  ('1R0N3LGNRJ', 36, 42053, 92357, 827, 83, 1, 23.94, 48, 0.2, 'Bachelor's', 'Self-employed', 'Divorced', 'Yes', 'No', 'Education', 'No', 1, 38.66, 'Low Risk', '2025-01-15'),
  ('CM9L1GTT2P', 40, 132784, 228510, 480, 114, 4, 9.09, 48, 0.33, 'High School', 'Self-employed', 'Married', 'Yes', 'No', 'Other', 'Yes', 0, 48.67, 'Medium Risk', '2025-01-15'),
  ('IA35XVH6ZO', 28, 140466, 163781, 652, 94, 2, 9.08, 48, 0.23, 'High School', 'Unemployed', 'Married', 'No', 'No', 'Education', 'No', 0, 31.36, 'Low Risk', '2025-01-15'),
  ('Y8UETC3LSG', 28, 149227, 139759, 375, 56, 3, 5.84, 36, 0.8, 'PhD', 'Full-time', 'Divorced', 'No', 'No', 'Education', 'Yes', 1, 60.07, 'Medium Risk', '2025-01-15'),
  ('RM6QSRHIYP', 41, 23265, 63527, 829, 87, 4, 9.73, 60, 0.45, 'Master's', 'Full-time', 'Divorced', 'Yes', 'No', 'Auto', 'Yes', 0, 39.53, 'Low Risk', '2025-01-15'),
  ('GX5YQOGROM', 53, 117550, 95744, 395, 112, 4, 3.58, 24, 0.73, 'High School', 'Unemployed', 'Single', 'No', 'No', 'Auto', 'Yes', 0, 55.59, 'Medium Risk', '2025-01-15'),
  ('X0BVPZLDC0', 57, 139699, 88143, 635, 112, 4, 5.63, 48, 0.2, 'Master's', 'Part-time', 'Divorced', 'No', 'No', 'Home', 'No', 0, 24.29, 'Low Risk', '2025-01-15'),
  ('O5DM5MPPNA', 41, 74064, 230883, 432, 31, 2, 5.0, 60, 0.89, 'Master's', 'Unemployed', 'Married', 'Yes', 'No', 'Auto', 'No', 0, 77.78, 'High Risk', '2025-01-15'),
  ('ZDDRGVTEXS', 20, 119704, 25697, 313, 49, 1, 9.63, 24, 0.28, 'PhD', 'Unemployed', 'Single', 'Yes', 'No', 'Home', 'No', 0, 49.06, 'Medium Risk', '2025-01-15'),
  ('9V0FJW7QPB', 39, 33015, 10889, 811, 106, 2, 13.56, 60, 0.66, 'Master's', 'Self-employed', 'Single', 'Yes', 'No', 'Other', 'No', 0, 42.13, 'Medium Risk', '2025-01-15'),
  ('O1IKKLC69B', 19, 40718, 78515, 319, 119, 2, 14.0, 24, 0.17, 'Bachelor's', 'Self-employed', 'Divorced', 'Yes', 'No', 'Education', 'No', 1, 60.64, 'Medium Risk', '2025-01-15'),
  ('F7487UU2BF', 41, 123419, 161146, 376, 65, 4, 16.96, 60, 0.39, 'High School', 'Self-employed', 'Single', 'Yes', 'No', 'Other', 'Yes', 0, 59.3, 'Medium Risk', '2025-01-15'),
  ('7ASF0IHRIT', 61, 30142, 133714, 429, 96, 1, 15.58, 12, 0.65, 'PhD', 'Part-time', 'Divorced', 'No', 'Yes', 'Business', 'No', 0, 74.13, 'High Risk', '2025-01-15'),
  ('A22KI1B6SE', 47, 146113, 100621, 419, 55, 1, 9.32, 12, 0.38, 'Bachelor's', 'Unemployed', 'Married', 'Yes', 'Yes', 'Business', 'No', 0, 45.84, 'Medium Risk', '2025-01-15'),
  ('1MUSHWD9TW', 55, 132058, 130912, 583, 48, 4, 5.82, 60, 0.47, 'High School', 'Unemployed', 'Married', 'No', 'Yes', 'Business', 'Yes', 0, 41.55, 'Medium Risk', '2025-01-15'),
  ('LXK7UEMLK0', 19, 118989, 123300, 528, 73, 3, 15.29, 36, 0.22, 'PhD', 'Part-time', 'Single', 'Yes', 'No', 'Business', 'Yes', 1, 42.94, 'Medium Risk', '2025-01-15'),
  ('995RE1TIB4', 38, 56848, 168918, 468, 73, 1, 19.1, 24, 0.22, 'Bachelor's', 'Unemployed', 'Single', 'No', 'No', 'Education', 'No', 0, 61.08, 'Medium Risk', '2025-01-15'),
  ('D17PDP8LBL', 50, 81649, 78193, 839, 110, 1, 21.41, 48, 0.5, 'Master's', 'Part-time', 'Married', 'Yes', 'No', 'Business', 'Yes', 0, 36.38, 'Low Risk', '2025-01-15'),
  ('C35RYEXWJ0', 29, 114651, 197648, 343, 58, 3, 21.07, 24, 0.19, 'Bachelor's', 'Part-time', 'Married', 'Yes', 'No', 'Home', 'Yes', 0, 61.89, 'Medium Risk', '2025-01-15'),
  ('G8AIMX5E52', 39, 17633, 167105, 514, 62, 3, 7.86, 36, 0.66, 'High School', 'Full-time', 'Single', 'Yes', 'Yes', 'Auto', 'Yes', 1, 71.53, 'High Risk', '2025-01-15'),
  ('BJNLQ0H95H', 61, 62519, 29676, 462, 16, 1, 23.91, 48, 0.12, 'Bachelor's', 'Unemployed', 'Divorced', 'Yes', 'No', 'Home', 'Yes', 0, 53.52, 'Medium Risk', '2025-01-15'),
  ('YIGLFWKNH5', 42, 141412, 197764, 580, 57, 2, 10.18, 12, 0.19, 'Bachelor's', 'Full-time', 'Married', 'No', 'No', 'Education', 'No', 0, 38.76, 'Low Risk', '2025-01-15'),
  ('GAA8OQN796', 66, 39568, 58945, 604, 37, 4, 6.67, 12, 0.1, 'High School', 'Unemployed', 'Divorced', 'Yes', 'Yes', 'Auto', 'Yes', 0, 41.26, 'Medium Risk', '2025-01-15'),
  ('P3EX8G0AYT', 44, 100284, 225403, 551, 31, 1, 18.77, 36, 0.17, 'Master's', 'Unemployed', 'Divorced', 'No', 'Yes', 'Business', 'Yes', 1, 53.43, 'Medium Risk', '2025-01-15'),
  ('KD97QJJFD8', 59, 102292, 55337, 840, 6, 1, 16.11, 60, 0.44, 'Master's', 'Unemployed', 'Married', 'Yes', 'No', 'Auto', 'No', 0, 32.16, 'Low Risk', '2025-01-15'),
  ('O8G74YT5W3', 45, 85673, 48773, 787, 103, 4, 22.42, 24, 0.82, 'PhD', 'Full-time', 'Divorced', 'No', 'No', 'Education', 'Yes', 0, 46.21, 'Medium Risk', '2025-01-15'),
  ('1CW0O8WTLF', 33, 92448, 66282, 607, 39, 1, 11.31, 12, 0.43, 'Bachelor's', 'Part-time', 'Divorced', 'Yes', 'No', 'Home', 'Yes', 0, 44.09, 'Medium Risk', '2025-01-15'),
  ('PBQO9E6L9D', 32, 102178, 179279, 669, 51, 2, 14.98, 12, 0.78, 'Master's', 'Self-employed', 'Single', 'Yes', 'No', 'Home', 'Yes', 0, 56.87, 'Medium Risk', '2025-01-15'),
  ('8NTWNU4HTY', 64, 102463, 218433, 506, 24, 2, 9.23, 60, 0.86, 'Master's', 'Unemployed', 'Married', 'No', 'No', 'Auto', 'Yes', 0, 69.61, 'Medium Risk', '2025-01-15'),
  ('ASYFXCP452', 68, 85409, 44772, 540, 105, 1, 2.96, 36, 0.17, 'Master's', 'Unemployed', 'Divorced', 'No', 'No', 'Auto', 'No', 0, 33.89, 'Low Risk', '2025-01-15'),
  ('4857M8R0YI', 61, 26470, 19818, 695, 47, 2, 20.0, 36, 0.69, 'PhD', 'Unemployed', 'Divorced', 'Yes', 'Yes', 'Auto', 'Yes', 0, 57.01, 'Medium Risk', '2025-01-15'),
  ('5FENBP2UV8', 69, 87295, 16281, 707, 94, 1, 13.82, 60, 0.75, 'Bachelor's', 'Part-time', 'Single', 'No', 'No', 'Other', 'No', 0, 43.03, 'Medium Risk', '2025-01-15'),
  ('EGBQ6R80VB', 20, 139321, 43049, 458, 117, 2, 7.16, 36, 0.53, 'Master's', 'Self-employed', 'Single', 'Yes', 'No', 'Home', 'No', 1, 41.08, 'Medium Risk', '2025-01-15'),
  ('RT511ZZNF2', 54, 21487, 84115, 386, 32, 2, 20.89, 60, 0.78, 'PhD', 'Unemployed', 'Married', 'Yes', 'Yes', 'Business', 'Yes', 0, 83.6, 'High Risk', '2025-01-15'),
  ('KAC7P2RE1X', 68, 111716, 215851, 747, 99, 3, 18.84, 60, 0.4, 'High School', 'Unemployed', 'Divorced', 'No', 'Yes', 'Home', 'No', 0, 42.65, 'Medium Risk', '2025-01-15'),
  ('XG5WPXX0TY', 24, 105732, 33013, 400, 58, 3, 15.64, 36, 0.48, 'High School', 'Full-time', 'Married', 'Yes', 'Yes', 'Auto', 'Yes', 0, 54.41, 'Medium Risk', '2025-01-15'),
  ('2XUD7N4OJ1', 38, 73117, 221628, 639, 84, 4, 13.05, 36, 0.56, 'Bachelor's', 'Part-time', 'Married', 'No', 'Yes', 'Home', 'Yes', 0, 57.74, 'Medium Risk', '2025-01-15'),
  ('2O7VM6EN0D', 26, 125268, 67671, 795, 72, 3, 11.42, 12, 0.3, 'High School', 'Self-employed', 'Single', 'No', 'No', 'Business', 'No', 0, 23.05, 'Low Risk', '2025-01-15'),
  ('Z7UFZIW3MK', 56, 24796, 37657, 498, 88, 1, 8.2, 48, 0.73, 'High School', 'Part-time', 'Married', 'Yes', 'Yes', 'Auto', 'No', 0, 63.7, 'Medium Risk', '2025-01-15'),
  ('RSP1YD80Z7', 35, 95963, 77552, 560, 8, 2, 6.63, 24, 0.86, 'Master's', 'Self-employed', 'Divorced', 'Yes', 'Yes', 'Home', 'No', 1, 57.75, 'Medium Risk', '2025-01-15'),
  ('AFJTOPMFJV', 21, 16170, 66883, 505, 27, 1, 4.41, 48, 0.42, 'PhD', 'Self-employed', 'Single', 'No', 'Yes', 'Education', 'No', 0, 59.26, 'Medium Risk', '2025-01-15'),
  ('LMQOF5PTTT', 42, 143244, 240591, 393, 96, 3, 23.15, 36, 0.16, 'High School', 'Self-employed', 'Married', 'No', 'Yes', 'Business', 'No', 0, 55.7, 'Medium Risk', '2025-01-15');


-- Verification Queries
SELECT 'kpi_summary' AS tbl, COUNT(*) AS rows FROM kpi_summary
UNION ALL SELECT 'loan_default_analytics', COUNT(*) FROM loan_default_analytics
UNION ALL SELECT 'employment_risk_summary', COUNT(*) FROM employment_risk_summary
UNION ALL SELECT 'financial_risk_summary', COUNT(*) FROM financial_risk_summary
UNION ALL SELECT 'customer_risk_score', COUNT(*) FROM customer_risk_score;
