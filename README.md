Airbnb Bangkok ETL Project
A simple and efficient data pipeline to ingest, clean, and load Airbnb datasets using PySpark and PostgreSQL.

Project Overview
This project automates the process of moving Airbnb data from raw CSV files into a structured PostgreSQL database. It focuses on data consistency and easy environment setup using Docker.

System Flow
The pipeline follows these 3 clear steps:

1.Ingestion: Reads raw CSV data from the ./dataset/raw/ directory into PySpark.

2.Transformation:

- Handles missing values (imputation) for the listings dataset.

- Ensures data types are correct before loading.

3.Loading: Exports the processed data into PostgreSQL tables via JDBC connectivity.

Tech Stack
- **Language:** Python 3.11
- **Data Processing:** PySpark
- **Database:** PostgreSQL (Containerized)
- **Infrastructure:**  Docker & Docker Compose (for infrastructure)
- **Management:** pgAdmin 4 (for database monitoring)

