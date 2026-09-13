# Databricks notebook source

dbutils.widgets.text("action", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_start_time", "")
dbutils.widgets.text("pipeline_status", "")

action = dbutils.widgets.get("action")
run_id = dbutils.widgets.get("run_id")
run_start_time = dbutils.widgets.get("run_start_time")
pipeline_status = dbutils.widgets.get("pipeline_status")

if action == "start":
    spark.sql(f"""
        INSERT INTO atliq.gold.pipeline_audit
        VALUES (
            '{run_id}',
            TIMESTAMP('{run_start_time}'),
            NULL,
            'RUNNING'
        )
    """)

elif action == "end":

    if pipeline_status.lower() == "success":
        row_count = spark.table("atliq.gold.fact_sales").count()
        status = "SUCCESS"
    else:
        row_count = "NULL"
        status = "FAILED"

    spark.sql(f"""
        UPDATE atliq.gold.pipeline_audit
        SET fact_sales_row_count = {row_count},
            status = '{status}'
        WHERE run_id = '{run_id}'
    """)

else:
    raise ValueError(f"Unknown audit action: {action}")