import openmeteo_requests
import json
import pandas as pd
import requests_cache
from retry_requests import retry
from requests import RequestException
import logging
from airflow.sdk import DAG

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

def weather_request():

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
		logger.error(f"Error en la peti {e}")
	except Exception as e:
		logger.error(f"Error {e}")

