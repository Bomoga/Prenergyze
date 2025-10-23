'''
Developers: Khanh Truong, Rhode Sanchez
Debugging: Adrian Morton
'''
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from pathlib import Path
import os

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
lat = 28.084358
lon = -82.372894
start_date = "2023-09-01"
end_date="2025-09-01"

## Set up Open-Meteo API client
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def fetch_openmeteo(lat, lon, start_date, end_date, hourly_vars):
    params = {
	"latitude": lat,
	"longitude": lon,
	"start_date": start_date,
	"end_date": end_date,
	"hourly": hourly_vars
    }

    responses = openmeteo.weather_api(BASE_URL, params=params)

    response = responses[0]

    print(f"Coordinates: {response.Latitude}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_dataframe = pd.DataFrame(data = hourly_data)
    
    return hourly_dataframe
    
vars_ = [
    "temperature_2m", "relative_humidity_2m", "apparent_temperature",
    "precipitation", "pressure_msl", "cloud_cover",
    "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "et0_fao_evapotranspiration", "vapour_pressure_deficit",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"
]

BASE_DIR = Path(__file__).resolve().parent.parent

output_path = Path(os.path.join(f"{BASE_DIR}","data", "raw", "meteo", f"METEO_{lat}_{lon}_{start_date}_{end_date}.csv"))
output_path.parent.mkdir(parents=True, exist_ok = True)

df = fetch_openmeteo(lat, lon, start_date, end_date, vars_)
df.to_csv(output_path)
print(df.describe())
print(df.isna().sum())
