The Healthcare Revenue Cycle Management (RCM) Data Pipeline is an end-to-end solution designed to streamline the ingestion, cleaning, refining, and transformation of healthcare data. This pipeline processes diverse data sources such as EMR (Electronic Medical Records), claims, and health codes data to generate usable, structured data for business stakeholders. It follows a Medallion Architecture with data stored in Azure Data Lake Storage Gen2 and processed using Databricks and PySpark.

The pipeline enables data processing in three layers:

Bronze Layer: Raw data collection.
Silver Layer: Cleaned and enriched data.
Gold Layer: Data structured into fact and dimension tables.
The pipeline supports both full and incremental data loads with consistent logging for auditing purposes.
