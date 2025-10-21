#made by khanh, rhode
import requests
import pandas as pd
from pathlib import Path

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_openmeteo(lat, lon, start_date, end_date, hourly_vars, timezone="UTC"):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,       # 'YYYY-MM-DD'
        "hourly": ",".join(hourly_vars),
        "timezone": timezone,
    }
    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    if "hourly" not in data or "time" not in data["hourly"]:
        raise RuntimeError(f"Open-Meteo error/empty payload:\n{data}")

    h = data["hourly"]
    # build DF
    df = pd.DataFrame({"datetime": pd.to_datetime(h["time"])})
    for v in hourly_vars:
        if v not in h:
            # variable not returned for this range/model; fill NaN column so shapes match
            df[v] = pd.Series([pd.NA]*len(df))
        else:
            df[v] = h[v]
    return df.set_index("datetime").sort_index()


# -------- usage --------
vars_ = [
    "temperature_2m", "relative_humidity_2m", "apparent_temperature",
    "precipitation", "pressure_msl", "cloud_cover",
    "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "et0_fao_evapotranspiration", "vapour_pressure_deficit",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"
]

df = fetch_openmeteo(
    lat=28.084358, lon=-82.372894,
    start_date="2023-01-01", end_date="2025-10-20",
    hourly_vars=vars_,
    timezone="UTC"
)

out_path = Path("data/raw/nasa/nasa.csv")
out_path.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(out_path)
print(df.describe())
print(df.isna().sum())
