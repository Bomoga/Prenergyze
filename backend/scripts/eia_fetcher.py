'''
Usage: 
Run the script. 
Input respondent code to specify which grid operator (FPL, MISO, etc.) you want to extract data from.

Authored by: 
Adrian Morton
'''
## Core imports
import os, requests
from pathlib import Path
import pandas as pd

## US EIA API Access
API_KEY = os.environ.get("EIA_API_KEY", "mq7cQLfepEbZ674BT2NOHHvhMs0pzbglrXM3Gdfn")
BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

FREQUENCY = "hourly"
REGION = "FPL"
START = "2019-01-01T00"
END = "2025-09-20T00"

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

def main():
    BASE_DIR = Path(__file__).resolve().parent.parent

    REGION = input("Input respondent code to acquire data from...")

    output_path = Path(os.path.join(f"{BASE_DIR}","data", "raw", "EIA", f"{REGION}_DEMAND_{START}_{END}.csv"))

    ## Load cache if data has already been extracted, fetch it if not
    if output_path.exists():
        data = pd.read_csv(output_path)
        print(f"Loaded cache from {output_path} into dataframe.")

    else:
        data = fetch(FREQUENCY, REGION, START, END)

        ## Load raw data into raw folder
        data.to_csv(output_path, index = False)

if __file__ == "__main__":
    main()