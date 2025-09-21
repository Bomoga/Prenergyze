#Core imports
import os, requests, json, time, csv
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

#US EIA API Access
API_KEY = os.environ.get("EIA_API_KEY", "mq7cQLfepEbZ674BT2NOHHvhMs0pzbglrXM3Gdfn")
BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

FREQUENCY = "hourly"
REGION = "FPL"
START = "2019-01-01T00"
END = "2019-01-03T00"

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
            "data[]": "value",
            "facets[type][]": "D",
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
            rows = data.get("response", {}).get("data", [])

        except Exception as e:
            print(f"Error fetching data from API.", e)
            return None

        resp = data.get("response", {})
        rows = data.get("data", [])

        if rows:
            print("Sample keys:", list(rows[0].keys()))

        total = resp.get("total", 0)

        if not rows:
                break
        
        if len(rows) <= 3:
            first = rows[0]
            if not isinstance(first, dict) or "period" not in first:
                print("API returned metadata instead of timeseries rows. Check params.")
                break

        df = pd.DataFrame(rows)
        frames.append(df)
        offset += length
        
        if (total and offset >= total) or (len(rows) < length):
             break
        
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index = True)

data = fetch(FREQUENCY, REGION, START, END)
print("Test rows:", len(data))
print(data.head())

