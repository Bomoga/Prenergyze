
# Core imports
import os
import requests
import csv
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
url = url = "https://power.larc.nasa.gov/api/temporal/hourly/point"


def fetch(start, end, latitude, longitude, community, parameters):
    s = requests.Session()

    # sanitize parameters: ensure comma-separated, no spaces
    if isinstance(parameters, str):
        parameters = ",".join(p.strip()
                              for p in parameters.split(",") if p.strip())

    params = {
        "start": start,
        "end": end,
        "latitude": latitude,
        "longitude": longitude,
        "community": community,
        "parameters": parameters,
        "time-standard": "utc",
        "format": "json",
    }

    # GET request
    r = s.get(url, params=params, timeout=60)  # get request
    if r.status_code != 200:
        print(f"HTTP {r.status_code} returned from API")
        # print response body for debugging
        try:
            print(r.text)
        except Exception:
            print("(no response body)")
    r.raise_for_status()  # check if something went wrong
    j = r.json()  # j is the entire json dictionary from nasa

    # Extract the actual hourly data block
    props = j.get("properties", {})
    param_block = props.get("parameter", {})

    # Collect all timestamps and variable names
    timestamps = sorted({t for series in param_block.values()
                        for t in series.keys()})
    variables = sorted(param_block.keys())

    data = []  # list
    for ts in timestamps:  # loop through each hour
        row = {"datetime_utc": ts}  # start a new row dict with the time
        for var in variables:
            # Add each variable’s value at that hour
            row[var] = param_block[var].get(ts, "")
        data.append(row)

    # Make sure output folder exists and generate a unique filename per run
    out_dir = Path("data/raw/nasa")
    out_dir.mkdir(parents=True, exist_ok=True)

    # create a filename that includes start/end and a short UTC timestamp
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # sanitize lat/lon for filename (remove minus and replace dot)
    lat_s = str(latitude).replace('-', 'm').replace('.', 'p')
    lon_s = str(longitude).replace('-', 'm').replace('.', 'p')
    filename = f"nasa_{start}_{end}_{lat_s}_{lon_s}_{now}.csv"
    out_path = out_dir / filename

    # Write to CSV
    with open(out_path, "w", newline="") as f:
        # Combine the timestamp column with all variable names for the CSV header
        writer = csv.DictWriter(f, fieldnames=["datetime_utc"] + variables)
        writer.writeheader()
        writer.writerows(data)

    print(f"saved {len(data)} rows")
    return data


fetch(
    start="20230101",
    end="20231231",
    latitude=27.6648,
    longitude=-81.5158,
    community="re",
    parameters="PRECTOTCORR,T2M,RH2M,WS2M,ALLSKY_SFC_SW_DWN,PS,TS,QV2M,GWETTOP,GWETROOT"
)
