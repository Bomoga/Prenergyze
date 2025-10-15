
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

        # Make sure output folder exists
        out_path = "data/raw/nasa/nasa.csv"
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)


    # Write to CSV
    with open(out_path, "w", newline="") as f:
        # Combine the timestamp column with all variable names for the CSV header
        writer = csv.DictWriter(f, fieldnames=["datetime_utc"] + variables)
        writer.writeheader()
        writer.writerows(data)

    print(f"saved {len(data)} rows")
    return data

fetch(
    start="20250101",
    end="20250107",
    latitude=25.7617,
    longitude=-80.1918,
    community="re",
    parameters="T2M,WS2M,RH2M,PRECTOTCORR"
)