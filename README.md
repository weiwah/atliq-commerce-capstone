# AtliQ Commerce Data Engineering Capstone

## Overview

An end-to-end data engineering project that builds a nightly data pipeline from operational data to an analytical dashboard.

## Architecture

Azure SQL + CSV  
→ Azure Data Factory  
→ ADLS Gen2 Bronze  
→ Azure Databricks Silver  
→ dbt Gold  
→ Microsoft Fabric / Power BI

## Project Milestones

### M1 — OLTP Database

Built the source database in Azure SQL, including seed data, an ETL control table, and a Python transaction simulator.

### M2 — Bronze Ingestion

Built an Azure Data Factory pipeline to load SQL and CSV data into ADLS Gen2 using full and incremental loads.

### M3 — Silver Transformation

Used PySpark and Delta Lake in Azure Databricks to clean, de-duplicate, and merge Bronze data into Silver tables.

### M4 — Gold Star Schema

Used dbt Core to build `fact_sales`, `dim_customer`, `dim_product`, and `dim_date`, with data-quality tests.

### M5 — Nightly Pipeline

Automated the Bronze → Silver → Gold pipeline and verified that repeated runs produce consistent results.

### M6 — Fabric Dashboard

Built a Power BI dashboard in Microsoft Fabric for revenue, products, cities, and customer analysis.

### M7 — CI/CD and Reliability

Added GitHub Actions CI, dbt testing, failure notifications, and pipeline audit logging.

## Project Structure

```text
atliq-commerce-capstone/
├── .github/workflows/    # GitHub Actions
├── adf/                  # ADF deployment files
├── atliq_gold/           # dbt project
├── data_assets/          # SQL, CSV and simulator
├── databricks/           # PySpark and audit code
├── atliq_commerce_architecture.svg
└── README.md
```

## Technologies

Azure SQL · Azure Data Factory · ADLS Gen2 · Azure Databricks · PySpark · Delta Lake · dbt Core · Microsoft Fabric · Power BI · GitHub Actions