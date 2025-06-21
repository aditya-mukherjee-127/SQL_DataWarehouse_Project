USE Data_Warehouse;

IF OBJECT_ID('logs.bronze_logs', 'U') IS NOT NULL
    DROP TABLE logs.bronze_logs;
CREATE TABLE logs.bronze_logs (
    log_id BIGINT,
    log_message NVARCHAR(1000),
    log_status NVARCHAR(1000),
    log_timestamp DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('logs.silver_logs', 'U') IS NOT NULL
    DROP TABLE logs.silver_logs;
CREATE TABLE logs.silver_logs (
    log_id BIGINT,
    log_message NVARCHAR(1000),
    log_status NVARCHAR(1000),
    log_timestamp DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('logs.gold_logs', 'U') IS NOT NULL
    DROP TABLE logs.gold_logs;
CREATE TABLE logs.gold_logs (
    log_id BIGINT,
    log_message NVARCHAR(1000),
    log_status NVARCHAR(1000),
    log_timestamp DATETIME DEFAULT GETDATE()
);