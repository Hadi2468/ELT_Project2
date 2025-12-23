## ELT_Project2: Healthcare Data Analysis

### Overview:
This project implements a cost-efficient, serverless batch data pipeline leveraging AWS Lambda, AWS Glue, and Amazon S3, designed around a Bronze–Silver–Gold lakehouse architecture. Raw data is ingested and preserved in the Bronze layer for auditability and traceability, transformed and standardized in the Silver layer, and stored in the Gold layer as analytics-ready Parquet datasets optimized for performance and scalability. Streamlit consumes these curated Parquet files directly from S3, removing the need for additional query engines such as Athena or Redshift while maintaining a simple, flexible, and easily extensible architecture.

![System Design](Project2_system_design.png)


