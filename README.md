# Event-Driven Serverless ETL on AWS
This repository contains source code for a scalable, __event-driven, serverless ETL pipeline__ built on AWS. The solution leverages AWS Lambda, AWS Glue Data Catalog for Iceberg tables, and AWS Athena to automate data ingestion, validation, transformation, and querying.

### High level architecture
![f1-arch-diag](https://github.com/user-attachments/assets/87af0b1a-91c1-4472-ab06-f9cb5d2b62e0)


### Features
- __Serverless Architecture__: Utilizes AWS Lambda, Athena and Glue to process data with no infrastructure management.
- __Event-Driven__: Triggered by S3 events or CloudWatch rules for real-time or scheduled execution.
- __Data Catalog Integration__: Uses AWS Glue for centralized schema management for Iceberg tables and views.
- __Query Layer__: Athena is used to query transformed datasets efficiently.
- __Modular Codebase__: Clearly separated ETL logic, validation, and SQL assets for ease of maintenance.

### Technologies Used
- Python
- AWS Lambda
- AWS Glue Data Catalog
- AWS Athena
- Amazon S3
- PyArrow 
- PyIceberg

### Setup & Deployment (High-Level)
- Upload CSV files to a designated S3 bucket.
- Configure S3 triggers or CloudWatch events to invoke Lambda.
- Lambda runs the ETL process:
    -    Reads files
    -    Validates schema
    -    Transforms and stores data in Iceberg tables
    -    Registers/updates tables in the Glue catalog
    -    Data is available for querying via Athena.


Ensure appropriate IAM roles are configured for Lambda to access Glue, S3, and Athena.

__Future update__: SQL execution will be saperate from this ETL logic. Probably using dbt or another lambda function.


