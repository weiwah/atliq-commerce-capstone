# AtliQ Commerce Architecture

## Architecture Overview

The AtliQ Commerce project uses a layered data architecture to move operational data from source systems into an analytical platform.

The overall data flow is:

Azure SQL + CSV  
→ Azure Data Factory  
→ ADLS Gen2 Bronze  
→ Azure Databricks Silver  
→ dbt Gold  
→ Microsoft Fabric / Power BI

Azure SQL serves as the OLTP system for operational transaction data. The Bronze, Silver, and Gold layers form the analytical side of the architecture, where data is progressively ingested, cleaned, transformed, and prepared for reporting.

## OLTP Source

Azure SQL Database is used as the OLTP source system. It contains normalized tables for customers, products, orders, order items, and payments.

A Python transaction simulator generates additional daily transactions to simulate changes in the operational system. An ETL control table stores metadata and watermarks used to determine how each source table should be loaded.

CSV files are also used as additional source data for the pipeline.

## Bronze Layer

Azure Data Factory (ADF) is responsible for ingesting data from Azure SQL and CSV sources into the Bronze layer in Azure Data Lake Storage Gen2.

The pipeline is metadata-driven so that one generic ingestion process can handle multiple source tables. Depending on the source, data is processed using either a full load or an incremental load.

Incremental loads use watermarks to identify new or changed records. Bronze data is stored as Parquet files and organized by ingestion date.

## Silver Layer

Azure Databricks and PySpark transform Bronze data into the Silver layer.

The Silver layer cleans and de-duplicates the source data before storing it as Delta tables. Full-load datasets are refreshed from the latest Bronze snapshot, while incremental datasets use Delta processing to apply new or changed records.

A `run_date` parameter determines which Bronze batch should be processed. The incremental processing is designed to be idempotent so that processing the same batch again does not create duplicate records.

Unity Catalog is used to manage the Silver and Gold tables and their access to cloud storage.

## Gold Layer

dbt Core transforms Silver data into a Gold star schema for analytics.

The Gold model contains:

- `fact_sales`
- `dim_customer`
- `dim_product`
- `dim_date`

dbt staging models provide a clean layer between Silver source tables and the final Gold models. dbt tests are used to validate important data-quality rules such as unique keys, non-null values, and relationships between fact and dimension tables.

## Reporting Layer

The Gold Delta tables are exposed to Microsoft Fabric through OneLake shortcuts, allowing Fabric to access the analytical data without creating another copy of the Gold datasets.

A Power BI semantic model connects the fact and dimension tables. The dashboard provides analysis of revenue, products, cities, and new versus returning customers, with date and product-category filtering.

## Nightly Synchronization

Azure Data Factory orchestrates the end-to-end nightly pipeline.

The execution flow is:

Azure SQL / CSV  
→ ADF Bronze ingestion  
→ Databricks Silver transformation  
→ dbt Gold build  
→ Fabric / Power BI

ADF first loads the latest source data into Bronze and then triggers the Databricks job. Databricks processes the corresponding Bronze batch into Silver before running the dbt Gold build.

The pipeline was tested for idempotency by executing the full process twice and confirming that the resulting `fact_sales` row count and total gross revenue remained unchanged.

## Reliability and CI/CD

Git and GitHub are used for source control. GitHub Actions runs `dbt build` on pull requests so that dbt models and data-quality tests can be validated before changes are merged.

The CI environment uses a separate Databricks schema and storage location to avoid affecting the production Gold tables.

The Databricks job also includes failure email notification and audit logging. Each pipeline run records its start time, final status, and `fact_sales` row count in the `pipeline_audit` Delta table.

Together, these components provide an end-to-end architecture that separates operational processing from analytical workloads while supporting incremental ingestion, repeatable processing, data-quality validation, monitoring, and automated reporting.