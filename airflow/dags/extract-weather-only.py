import os
import json
from datetime import datetime
import pandas as pd

import openmeteo_requests
import requests_cache
from retry_requests import retry
from requests import RequestException

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

shared_dir = "/opt/airflow/shared"
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "hourly": "temperature_2m",
}

def weather_request(url=url, params=params):
    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_dataframe = pd.DataFrame(data=hourly_data)

        temps_date_json = hourly_dataframe.to_json(orient="records", date_format="iso")
        full_data_json = {
            "latitud": response.Latitude(),
            "longitud": response.Longitude(),
            "data": temps_date_json
        }
        #Test
        logger.info(f" ESTE ES EL JSON DEL TIEMPO: {json.dumps(full_data_json)} ")

        daily_full_data_json_path = f"{shared_dir}/{datetime.now().strftime('%Y%m%d%H%M%S')}"
        os.makedirs(daily_full_data_json_path, exist_ok=True)

        with open(f"{daily_full_data_json_path}/weather_data.json", "w", encoding="utf-8") as archivo:
            json.dump(full_data_json, archivo, ensure_ascii=False, indent=4)

        logger.info("Weather data extracted successfully.")
        return daily_full_data_json_path

    except RequestException as e:
        logger.error(f"Error en la request: {e}")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")

dag = DAG(
    dag_id="extract_weather_only",
    schedule="@daily",
    start_date=datetime(2025, 11, 11),
    catchup=False
)

extract_weather_task = PythonOperator(
    task_id="extract_weather_task",
    python_callable=weather_request,
    dag=dag
)
