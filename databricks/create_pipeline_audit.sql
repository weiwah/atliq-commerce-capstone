CREATE TABLE IF NOT EXISTS atliq.gold.pipeline_audit (
    run_id STRING,
    run_start_time TIMESTAMP,
    fact_sales_row_count INT,
    status STRING
)
USING DELTA;