--- Create Database RegulatoryReporting
CREATE DATABASE RegulatoryReporting;
GO
USE RegulatoryReporting;
USE RegulatoryReporting;
GO



BULK INSERT accounts
FROM '/var/opt/mssql/accounts.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
BULK INSERT loans
FROM '/var/opt/mssql/loans.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
BULK INSERT liquidity
FROM '/var/opt/mssql/liquidity.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
USE RegulatoryReporting;
GO

CREATE TABLE liquidity (
    item_id INT,
    cash_in BIGINT,
    cash_out BIGINT,
    source_type VARCHAR(100),
    report_type VARCHAR(20)
);

BULK INSERT liquidity
FROM '/var/opt/mssql/liquidity.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
SELECT COUNT(*) FROM liquidity;

-- Y-9C: Aggregate key balance sheet items
SELECT
    account_type,
    SUM(amount) AS total_amount
FROM accounts
GROUP BY account_type
ORDER BY total_amount DESC;

-- Y-14: Calculate risk-weighted assets by loan type
SELECT
    loan_type,
    SUM(balance) AS total_balance,
    AVG(risk_weight) AS avg_risk_weight,
    SUM(balance * risk_weight) AS total_risk_weighted_assets
FROM loans
GROUP BY loan_type
ORDER BY total_risk_weighted_assets DESC;

-- 2052a: Net cash position by funding source
SELECT
    source_type,
    SUM(cash_in) AS total_cash_in,
    SUM(cash_out) AS total_cash_out,
    SUM(cash_in - cash_out) AS net_cash_flow
FROM liquidity
GROUP BY source_type
ORDER BY net_cash_flow DESC;
