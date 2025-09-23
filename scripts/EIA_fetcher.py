'''
Summary:
This script automates the extraction and preprocessing of EIA API data. 

Usage: 
Input respondent code to specify which grid operator (FPL, MISO, etc.) you want to extract data from.

Authored by: 
Adrian Morton
'''
## Core imports
import os 
import requests, json, time, csv
from pathlib import Path
import pandas as pd
import matplotlib
import numpy as np

## Set 'True' for debug info
INFO_MODE = False

## US EIA API Access
API_KEY = os.environ.get("EIA_API_KEY", "mq7cQLfepEbZ674BT2NOHHvhMs0pzbglrXM3Gdfn")
BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

FREQUENCY = "hourly"
REGION = "FPL"
START = "2019-01-01T00"
END = "2025-09-20T00"

def clean(data):
    ## Remove negatives
    data.loc[data['value'] < 0, 'value'] = np.nan

    ## Remove duplicates
    data.drop_duplicates(subset = 'period', keep = 'last', inplace = True)

    ## Handle outliers
    mean = data['value'].mean()
    std = data['value'].std()
    z = (data['value'] - mean) / std
    data.loc[abs(z) > 4, 'value'] = np.nan

    ## Convert period timestamps to DateTime object format
    data['period'] = pd.to_datetime(data['period'], format="%Y-%m-%dT%H")  
    data = data.set_index('period', drop = False)

    ## Fill back
    data = data.sort_index()
    data['value'] = data['value'].interpolate(method="time")

#Fetches data from API and returns a concatenated pandas dataframe
def fetch(frequency, region, start, end, length = 5000, session = None):
    frames = [] 
    offset = 0
    s = session or requests.Session()
    while True:
        params = {
            "api_key": API_KEY,
            "frequency": frequency,
            "start": start,
            "end": end,
            "offset": offset,
            "length": length,
            "data[0]": "value",
            "facets[type][0]": "D",
            "facets[respondent][0]": region,
            "sort[0][column]": "period",
            "sort[0][direction]" : "asc",
        }

        prepared = requests.Request("GET", BASE_URL, params = params).prepare()
        print("Fetching...", prepared.url)
        
        try:
            response = s.get(BASE_URL, params = params, timeout = 50)
            if not response.ok:
                print("HTTP", response.status_code, "-", response.reason)
                print("Body:", response.text[:500])
                break
            response.raise_for_status()
            data = response.json()
            resp = data.get("response", {})
            rows = data.get("response", {}).get("data", [])

        except Exception as e:
            print(f"Error fetching data from API.", e)
            return None

        if INFO_MODE:
            print(rows)
            print(resp)

        if rows and INFO_MODE:
            print("Sample keys:", list(rows[0].keys()))

        total = int(resp.get("total") or 0)

        if not rows:
                break
        
        if len(rows) <= 3:
            first = rows[0]
            if not isinstance(first, dict) or "period" not in first:
                print("API returned metadata instead of timeseries rows. Check params.")
                break

        df = pd.DataFrame(rows)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])

        frames.append(df)
        offset += length
        
        if (total and offset >= total) or (len(rows) < length):
             break
        
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index = True)

#Debug helper function to find out available parameters and respondents
def discover_available(frequency, start, end, length = 1000, session = None):
    s = session or requests.Session()
    params = {
        "api_key": API_KEY,
        "frequency": frequency,
        "start": start,
        "end": end,
        "offset": 0,
        "length": length,
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }

    prepared = requests.Request("GET", BASE_URL, params=params).prepare()
    print("Discover URL:", prepared.url)
    r = s.get(BASE_URL, params=params, timeout=60)
    print("HTTP", r.status_code)

    if not r.ok:
        print("Body:", r.text[:500])
        return pd.DataFrame()
    
    payload = r.json()
    rows = payload.get("response", {}).get("data", [])
    df = pd.DataFrame(rows)

    if df.empty:
        print("Discovery returned 0 rows - try widening the window or removing frequency/type filters.")
        return df
    
    print("Sample keys:", list(df.columns))

    if "respondent" in df.columns:
        print("Top respondents:")
        print(df["respondent"].value_counts().head(15))

    if "type" in df.columns:
        print("Types present:", df["type"].unique())

    return df

if INFO_MODE:
    disc = discover_available(FREQUENCY, REGION, START, END)
    disc.groupby(["respondent","type"]).size().sort_values(ascending=False).head(20)
    disc_ = disc[(disc["respondent"] == REGION) & (disc["type"] == "D")]
    print(len(disc_))
    disc_.head()

#########################################################################################################

## Obtain program directory
BASE_DIR = Path(__file__).resolve().parent.parent

REGION = input("Input respondent code to acquire data from...")

output_path = Path(os.path.join(f"{BASE_DIR}","data", "raw", "EIA", f"{REGION}_DEMAND_{START}_{END}.csv"))

## Load cache if data has already been extracted, fetch it if not
if output_path.exists():
    data = pd.read_csv(output_path)
    print(f"Loaded cache from {output_path} into dataframe.")

else:
    data = fetch(FREQUENCY, REGION, START, END)

    if INFO_MODE:
        print("Test rows:", len(data))
        print(data.head())
        print(data.shape)
        print(data.columns)

    ## Load raw data into raw folder
    data.to_csv(output_path, index = False)

clean(data)

output_path = Path(os.path.join(f"{BASE_DIR}","data","processed", "EIA", f"{REGION}_DEMAND_{START}_{END}.csv"))
    
## If cleaned data has not been loaded yet, load it into data/processed/EIA folder
if output_path.exists():
    print(f"Data already cleaned and loaded at {output_path}")
else:
    ## Load cleaned data into processed folder
    data.to_csv(output_path, index = False)