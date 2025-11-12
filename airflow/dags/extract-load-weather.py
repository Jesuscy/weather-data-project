import openmeteo_requests
import json
import pandas as pd
import requests_cache
from retry_requests import retry
from requests import RequestException
import logging

from airflow.sdk import DAG, PythonOperator
from airflow.operators.python_operator import PythonOperator 

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DataLakeFileClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 52.52,
	"longitude": 13.41,
	"hourly": "temperature_2m",
}



def weather_request(url, params):

	try : 
		responses = openmeteo.weather_api(url, params=params)

		response = responses[0]
		hourly = response.Hourly()
		hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

		hourly_data = {"date": pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
			end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"
		)}

		hourly_data["temperature_2m"] = hourly_temperature_2m
		hourly_dataframe = pd.DataFrame(data = hourly_data)

		temps_date_json = hourly_dataframe.to_json(orient="records", date_format="iso")
		full_data_json = {
			"latitud" : response.Latitude(),
			"longitud" : response.Longitude(),
			"data" : temps_date_json
		}
		
		return full_data_json
	
	except RequestException as e: 
		logger.error(f"Error en la request {e}")
	except Exception as e:
		logger.error(f"Error {e}")

	try:
		credential = ClientSecretCredential(
		tenant_id="",
		client_id="",
		client_secret=""
			)
		service_client = DataLakeServiceClient(account_url="", credential=credential)
		file_system_client = service_client.create_file_system_if_not_exists(file_system="weather-container-landing")
		file_client = file_system_client.create_file(f"weather_data_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.json")
		file_client.append_data(full_data_json, offset=0, length=len(full_data_json))
		file_client.flush_data(len(full_data_json))
		logger.info("Data uploaded to Data Lake successfully.")
	
	except Exception as e:	
		logger.error(f"Error uploading to Data Lake {e}")	


dag = DAG(
	dag_id = "extract_load_weather",
	schedule = "@daily",
	start_date = 2025-11-11,
	catchup = False
)

extract_weather_task = PythonOperator(
	task_id = "extract_weather_task",
	python_callable = weather_request(url=url, params=params),
	dag = dag
)

extract_weather_task