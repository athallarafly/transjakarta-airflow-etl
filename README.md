# Simple ETL Pipeline: Airflow to PostgreSQL

This project demonstrates a **simple ETL pipeline using Apache Airflow and PostgreSQL**.  
The pipeline extracts a dataset from Kaggle, performs basic data cleaning, and loads the processed data into a local PostgreSQL database.

## Dataset

Dataset source:  
https://www.kaggle.com/datasets/dikisahkan/transjakarta-transportation-transaction

This dataset contains **TransJakarta transportation transaction data**.

## Pipeline Overview

The ETL pipeline performs the following steps:

1. **Extract**
   - Read the dataset from a CSV file.

2. **Transform**
   - Convert all column names to lowercase.
   - Drop rows with null values.
   - Convert data types where necessary.

3. **Load**
   - Load the cleaned dataset into a **local PostgreSQL database**.

## Technologies Used

- Apache Airflow
- PostgreSQL
- Python
- Pandas
- Docker

## Notes

This project is intended as a simple ETL practice project using Airflow.
In a production environment, additional improvements would be needed, such as:

- Implementing upsert instead of append
- Adding data validation
- Handling large datasets
- Adding logging and monitoring
