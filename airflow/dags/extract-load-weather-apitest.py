import os
import json
from datetime import datetime, timedelta
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator 

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

shared_dir = "/opt/airflow/shared"


def weather_request_dummy():
    try:
        start_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        hours = 24
        hourly_data = {
            "date": pd.date_range(start=start_time, periods=hours, freq="H").strftime("%Y-%m-%dT%H:%M:%SZ").tolist(),
            "temperature_2m": [round(15 + i*0.5, 2) for i in range(hours)]
        }

        full_data_json = {
            "latitude": 52.52,
            "longitude": 13.41,
            "data": hourly_data
        }

        daily_full_data_json_path = f"{shared_dir}/{datetime.now().strftime('%Y%m%d%H%M%S')}"
        os.makedirs(daily_full_data_json_path, exist_ok=True)

        with open(f"{daily_full_data_json_path}/weather_data.json","w", encoding="utf-8") as archivo:
            json.dump(full_data_json, archivo, ensure_ascii=False, indent=4)
        logger.info("Weather dummy data created successfully.")

        return daily_full_data_json_path

    except Exception as e:
        logger.error(f"Error creating dummy weather data: {e}")
        raise


def upload_to_azure(**kwargs):
    try:
        credential = ClientSecretCredential(
            tenant_id=os.getenv("TENANT_ID"),
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("CLIENT_SECRET")
        )

        full_data_json_path = kwargs['ti'].xcom_pull(task_ids='extract_weather_task')

        with open(f"{full_data_json_path}/weather_data.json", "r", encoding="utf-8") as file:
            full_data_json = json.load(file)

        full_data_json_str = json.dumps(full_data_json)

        try:
            service_client = DataLakeServiceClient(account_url="", credential=credential)
            file_system_client = service_client.create_file_system_if_not_exists(file_system="weather-container-landing")
        except AzureError as e:
            logger.error(f"Error connecting to Data Lake: {e}")
            return

        try:
            file_client = file_system_client.create_file(f"weather_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
            file_client.append_data(full_data_json_str, offset=0, length=len(full_data_json_str))
            file_client.flush_data(len(full_data_json_str))
            logger.info("Data uploaded to Data Lake successfully.")
        except AzureError as e:
            logger.error(f"Error uploading file to Data Lake: {e}")

    except Exception as e:
        logger.error(f"Error in upload_to_azure: {e}")
        raise

dag = DAG(
    dag_id="extract_load_weather_dummy",
    schedule="@daily",
    start_date=datetime(2025, 11, 11),
    catchup=False
)

extract_weather_task = PythonOperator(
    task_id="extract_weather_task",
    python_callable=weather_request_dummy,
    dag=dag
)

upload_to_azure_task = PythonOperator(
    task_id="upload_to_azure_task",
    python_callable=upload_to_azure,
    dag=dag
)

extract_weather_task >> upload_to_azure_task
