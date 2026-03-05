from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
# import requests
from sqlalchemy import create_engine, text


# ================== CONFIG ==================
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "host.docker.internal"
DB_PORT = "5432"
DB_NAME = "etl_test"
SOURCE_CSV_PATH = "/opt/airflow/data/data_transjakarta_1.csv"
TABLE_NAME = "transjakarta_trx"


def extract_data_tj():

    df = pd.read_csv(SOURCE_CSV_PATH)
    df.to_csv("/opt/airflow/data/raw_tj.csv", index=False)
    print("Extract selesai")

def transform_data_tj():
    df = pd.read_csv("/opt/airflow/data/raw_tj.csv")

    #lowercase column
    df.columns = df.columns.str.lower()

    #drop null values
    df = df.dropna()

    #convert date
    df['tapintime'] = pd.to_datetime(df['tapintime'])
    df['tapouttime'] = pd.to_datetime(df['tapouttime'])

    #save to clean csv
    df.to_csv("/opt/airflow/data/clean_tj.csv", index=False)


def load_data_tj():
    df = pd.read_csv("/opt/airflow/data/clean_tj.csv")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    #append to table
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)


with DAG(
    dag_id="transjakarta_csv_etl",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "transfrom", "postgres"]
)as dag:

    extract_data_tj_task = PythonOperator(
        task_id="extract_data_tj",
        python_callable=extract_data_tj
    )

    transform_data_tj_task = PythonOperator(
        task_id="transform_data_tj",
        python_callable=transform_data_tj
    )

    load_data_tj_task = PythonOperator(
        task_id="load_data_tj",
        python_callable=load_data_tj
    )
    
    extract_data_tj_task >> transform_data_tj_task >> load_data_tj_task