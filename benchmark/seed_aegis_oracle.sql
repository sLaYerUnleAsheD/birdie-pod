-- =====================================================================
-- Aegis Modernize: SQL Server 2022 Oracle Benchmark Seed Script
-- Database: aegis_oracle
-- Populates ground-truth tables for tsql_01 through tsql_11
-- =====================================================================

USE aegis_oracle;
GO

-- 1. Table: dbo.policy
IF OBJECT_ID('dbo.policy', 'U') IS NOT NULL DROP TABLE dbo.policy;
CREATE TABLE dbo.policy (
    policy_id              NVARCHAR(40) PRIMARY KEY,
    product                NVARCHAR(50),
    premium                DECIMAL(12, 2),
    region                 NVARCHAR(50),
    status                 NVARCHAR(20),
    effective_date         DATE,
    earned_premium         DECIMAL(14, 2),
    currency               NVARCHAR(10),
    gross_written_premium  DECIMAL(18, 2),
    inception_date         DATE,
    expiry_date            DATE,
    claim_units            INT,
    policy_count           INT
);

INSERT INTO dbo.policy 
(policy_id, product, premium, region, status, effective_date, earned_premium, currency, gross_written_premium, inception_date, expiry_date, claim_units, policy_count)
VALUES
('POL-001', 'COMMERCIAL_AUTO', 12500.00, 'DUBLIN',   'ACTIVE',    '2024-01-01', 12000.00, 'GBP', 12500.00, '2024-01-01', '2024-12-31', 2, 1),
('POL-002', 'HOMEOWNERS',       3400.50, 'MUNSTER',  'ACTIVE',    '2024-02-01',  3200.00, 'EUR',  3400.50, '2024-02-01', '2025-01-31', 1, 1),
('POL-003', 'MOTOR',            4200.00, 'CONNACHT', 'ACTIVE',    '2024-03-01',  4000.00, 'GBP',  4200.00, '2024-03-01', '2025-02-28', 3, 1),
('POL-004', 'COMMERCIAL_AUTO',  8500.00, 'LEINSTER', 'CANCELLED', '2024-01-15',  2100.00, 'USD',  8500.00, '2024-01-15', '2025-01-14', 0, 1),
('POL-005', 'HOMEOWNERS',       2100.00, 'DUBLIN',   'ACTIVE',    '2024-04-01',  2000.00, 'GBP',  2100.00, '2024-04-01', '2025-03-31', 1, 1),
('POL-006', 'MOTOR',            1500.00, 'MUNSTER',  'ACTIVE',    '2024-05-01',  1400.00, 'EUR',  1500.00, '2024-05-01', '2025-04-30', 2, 1),
('POL-007', 'LIABILITY',        5600.00, 'CONNACHT', 'ACTIVE',    '2024-06-01',  5200.00, 'GBP',  5600.00, '2024-06-01', '2025-05-31', 0, 1),
('POL-008', 'PROPERTY',         9800.00, 'LEINSTER', 'LAPSED',    '2023-11-01',  9800.00, 'USD',  9800.00, '2023-11-01', '2024-10-31', 1, 1);
GO

-- 2. Table: dbo.claim
IF OBJECT_ID('dbo.claim', 'U') IS NOT NULL DROP TABLE dbo.claim;
CREATE TABLE dbo.claim (
    claim_id         NVARCHAR(40) PRIMARY KEY,
    policy_id        NVARCHAR(40),
    loss_date        DATE,
    report_date      DATE,
    settlement_date  DATE,
    status           NVARCHAR(20),
    reserve          DECIMAL(12, 2),
    paid_loss        DECIMAL(14, 2),
    case_reserve     DECIMAL(14, 2),
    gross_incurred   DECIMAL(14, 2),
    region           NVARCHAR(50)
);

INSERT INTO dbo.claim 
(claim_id, policy_id, loss_date, report_date, settlement_date, status, reserve, paid_loss, case_reserve, gross_incurred, region)
VALUES
('CLM-101', 'POL-001', '2025-01-15', '2025-01-18', '2025-02-20', 'OPEN',    4500.00,  1500.00, 3000.00,  4500.00, 'DUBLIN'),
('CLM-102', 'POL-001', '2025-02-10', '2025-02-14', NULL,         'OPEN',    8200.50,  2200.50, 6000.00,  8200.50, 'DUBLIN'),
('CLM-103', 'POL-002', '2025-01-22', '2025-01-25', '2025-03-01', 'CLOSED',  3400.00,  3400.00,    0.00,  3400.00, 'MUNSTER'),
('CLM-104', 'POL-003', '2025-03-05', '2025-03-09', NULL,         'open',    5100.00,  1000.00, 4100.00,  5100.00, 'CONNACHT'),
('CLM-105', 'POL-005', '2025-02-18', '2025-02-20', NULL,         'OPEN',    1900.00,   500.00, 1400.00,  1900.00, 'DUBLIN'),
('CLM-106', 'POL-006', '2025-03-12', '2025-03-15', '2025-04-10', 'SETTLED', 6300.00,  6300.00,    0.00,  6300.00, 'MUNSTER'),
('CLM-107', 'POL-008', '2024-12-05', '2024-12-10', NULL,         'OPEN',   12500.00,  2500.00, 10000.00, 12500.00, 'LEINSTER');
GO

-- 3. Table: dbo.fx_rates
IF OBJECT_ID('dbo.fx_rates', 'U') IS NOT NULL DROP TABLE dbo.fx_rates;
CREATE TABLE dbo.fx_rates (
    currency_code  NVARCHAR(10),
    rate_date      DATE,
    gbp_rate       DECIMAL(10, 6),
    PRIMARY KEY (currency_code, rate_date)
);

INSERT INTO dbo.fx_rates (currency_code, rate_date, gbp_rate)
VALUES
('GBP', CAST(GETDATE() AS DATE), 1.000000),
('EUR', CAST(GETDATE() AS DATE), 0.855000),
('USD', CAST(GETDATE() AS DATE), 0.785000),
('EUR', DATEADD(day, -1, CAST(GETDATE() AS DATE)), 0.854000),
('USD', DATEADD(day, -1, CAST(GETDATE() AS DATE)), 0.784000);
GO

-- 4. Tables: dbo.policy_snapshot & dbo.policy_delta
IF OBJECT_ID('dbo.policy_snapshot', 'U') IS NOT NULL DROP TABLE dbo.policy_snapshot;
CREATE TABLE dbo.policy_snapshot (
    policy_id  NVARCHAR(40) PRIMARY KEY,
    product    NVARCHAR(50),
    premium    DECIMAL(12, 2),
    status     NVARCHAR(20)
);

INSERT INTO dbo.policy_snapshot (policy_id, product, premium, status)
VALUES
('POL-001', 'COMMERCIAL_AUTO', 12500.00, 'ACTIVE'),
('POL-002', 'HOMEOWNERS',       3400.50, 'ACTIVE'),
('POL-003', 'MOTOR',            4200.00, 'PENDING');
GO

IF OBJECT_ID('dbo.policy_delta', 'U') IS NOT NULL DROP TABLE dbo.policy_delta;
CREATE TABLE dbo.policy_delta (
    policy_id  NVARCHAR(40),
    product    NVARCHAR(50),
    premium    DECIMAL(12, 2),
    status     NVARCHAR(20),
    action     NVARCHAR(5)
);

INSERT INTO dbo.policy_delta (policy_id, product, premium, status, action)
VALUES
('POL-003', 'MOTOR',       4500.00, 'ACTIVE', 'U'),
('POL-004', 'COMMERCIAL',  9000.00, 'ACTIVE', 'I');
GO

-- 5. Verification Query: View dbo.policy
SELECT * FROM dbo.policy;
GO
