import json
import requests
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# API URL (Example: Fetching dummy data)
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=40.7128&longitude=-74.0060&daily=temperature_2m_max&timezone=auto"

# Function to fetch data from API
def fetch_api_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        print(response.json())
        return response.json()
    else:
        raise Exception("Failed to fetch API data")
    
# Function to insert data into PostgreSQL
def insert_into_postgres(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data_task')  # Get data from previous task
    print('data==')
    print(data)
    print(type(data))
    print(data['latitude'], data['longitude'], data['elevation'])
    if not data:
        raise ValueError("No data received from API")

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO weathers (id,latitude, longitude, elevation)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, (1,data['latitude'], data['longitude'], data['elevation']))
    # for record in data:
    #     print('record==')
    #     print(record)
    conn.commit()
    cursor.close()
    conn.close()
    
    
# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 18),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='fetch_api_insert_postgres',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_api_data,
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_into_postgres,
        provide_context=True,
    )

    # Task to create table
    create_table = PostgresOperator(
        task_id='create_weathers_table',
        postgres_conn_id='postgres_default',  # Connection ID from Airflow UI
        sql="""
            CREATE TABLE IF NOT EXISTS weathers (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                elevation NUMERIC(11,0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # Define task dependencies
    fetch_data_task >> create_table >> insert_data_task
