import os, requests
from pathlib import Path
import pandas as pd
import numpy as np
from io import StringIO

INFO_MODE = False

BASE_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"

PARAMETERS = "T2M,PRECTOT,WS2M,ALLSKY_SFC_SW_DWN,RH2M"
START = "20190101"
END = "20250920"
LAT = 26.5225
LON = 81.1637

def fetch(start, end, parameters, latitude, longitude, session = None):
    s = session or requests.Session()

    params = {
        "parameters": parameters,
        "latitude": latitude,
        "longitude": longitude,
        "community": "AG",
        "start": start,                    
        "end": end,                      
        "format": "CSV"
    }

    try:
        response = s.get(BASE_URL, params = params, timeout = 30)
        response.raise_for_status()

        return pd.read_csv(StringIO(response.text), skiprows = 13)

    except Exception as e:
        print(e)
        return None

#########################################################################################################

BASE_DIR = Path(__file__).resolve().parent.parent

output_path = Path(os.path.join(f"{BASE_DIR}","data", "raw", "NASA_POWER", f"{PARAMETERS}_{START}_{END}.csv"))

if output_path.exists():
    data = pd.read_csv(output_path)
    print(f"Loaded cache from {output_path} into dataframe.")

else:
    data = fetch(START, END, PARAMETERS, LAT, LON)

    if INFO_MODE:
        print("Test rows:", len(data))
        print(data.head())
        print(data.shape)
        print(data.columns)

    ## Load raw data into raw folder
    data.to_csv(output_path, index = False)