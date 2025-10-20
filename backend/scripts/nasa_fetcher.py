#this one is made by rhode
## Core imports
import os, requests
import csv
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
url = url = "https://power.larc.nasa.gov/api/temporal/hourly/point"



def fetch(start, end, latitude, longitude, community, parameters):
    s = requests.Session()

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
    
    #GET request
    r = s.get(url, params = params, timeout = 60) #get request
    r.raise_for_status() #check if sm went wrong
    j = r.json() #j is the entire json dictionary from nasa

    # Extract the actual hourly data block
    props = j.get("properties", {})
    param_block = props.get("parameter", {})

    # Collect all timestamps and variable names
    timestamps = sorted({t for series in param_block.values() for t in series.keys()})
    variables = sorted(param_block.keys())

    data =[] #list
    for ts in timestamps: #loop through each hour
        row = {"datetime_utc": ts}#start a new row dict with the time
        for var in variables:
            row[var] = param_block[var].get(ts, "") #Add each variable’s value at that hour
        data.append(row)

    # Make sure output folder exists and generate a unique filename per run
    out_dir = Path("data/raw/nasa")
    out_dir.mkdir(parents=True, exist_ok=True)

    # create a filename that includes start/end and a short UTC timestamp
    from datetime import datetime
    now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
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
    start = "20250101",
    end = "20251231",
    latitude = 27.6648,
    longitude = -81.5158,
    community = "re",
    parameters = "T2M,WS2M,RH2M,PRECTOTCORR"
)