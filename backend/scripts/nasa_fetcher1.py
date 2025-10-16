import requests
import pandas as pd
import os
from pathlib import Path
url = "https://power.larc.nasa.gov/api/temporal/daily/point"

params = {
    "start": "20190101",
    "end": "20251016",
    "latitude": 27.6648,
    "longitude": -81.5158,
    "community": "re",
    "parameters": "PRECTOTCORR,T2M,T2M_MAX,T2M_MIN,RH2M,WS2M,ALLSKY_SFC_SW_DWN,PS,TS,QV2M,GWETTOP,GWETROOT",
    "format": "CSV",
    "user": "KhanhTruong",
    "header": "true",
    "units": "metric"
}

# Call the API
response = requests.get(url, params=params)
response.raise_for_status()  # ensure request succeeded

with open("florida_nasa_power_2019_2025", "w", encoding="utf-8") as f:
    f.write(response.text)
path = "data/nasa/data/florida_nasa_power_2019_2025.csv"

with open(path) as f:
    for i in range(25):
        print(i, f.readline().rstrip())

df = pd.read_csv(path, skiprows=20,
                 on_bad_lines="skip", engine="python")
print(df.head())
print(df.info())
df = df.rename(columns={
    "YEAR": "year",
    "MO": "month",
    "DY": "day",
    "PRECTOTCORR": "precipitation__mm__day",
    "T2M": "temp_avg_C",
    "T2M_MAX": "temp_max_C",
    "T2M_MIN": "temp_min_C",
    "RH2M": "humidity_percent",
    "WS2M": "wind_speed_mps",
    "ALLSKY_SFC_SW_DWN": "solar_radiation_kwh_m2_day",
    "PS": "surface_pressure_kPa",
    "TS": "skin_temp_C",
    "QV2M": "specific_humidity_gkg",
    "GWETTOP": "soil_moisture_top_percent",
    "GWETROOT": "soil_moisture_root_percent"
})

df["date"] = pd.to_datetime(df[["year", "month", "day"]])
df = df.drop(columns=["year", "month", "day"])

print(df.isna().sum())
cols = ["date"] + [c for c in df.columns if c != "date"]
df = df[cols]

print(df.head())
df.to_csv(path, index=False)
