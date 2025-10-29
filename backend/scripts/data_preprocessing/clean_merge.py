import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path
import numpy as np

BASE_DIR = Path(os.getcwd()).parent.parent.parent
raw_eia_output_path = Path(os.path.join(f"{BASE_DIR}","backend", "data", "raw", "eia", "FPL_DEMAND_2023-09-01T00_2025-09-01T00.csv"))
raw_meteo_output_path = Path(os.path.join(f"{BASE_DIR}", "backend", "data", "raw", "meteo", "METEO_28.084358_-82.372894_2023-09-01_2025-09-01.csv"))

## Determine Z-score and categorize row as outlier or not
def process(row, mean, std):
    z = (row - mean)/std
    if abs(z) > 3:
        return None
    else:
        return row
    
def main():
    raw_eia_data = pd.read_csv(raw_eia_output_path)
    raw_meteo_data = pd.read_csv(raw_meteo_output_path)

    ## Drop null vals from both datasets

    raw_eia_data.dropna()
    raw_meteo_data.dropna()

    ## Drop dupes from both datasets

    raw_eia_data['period'].drop_duplicates()
    raw_meteo_data['date'].drop_duplicates()

    ## Remove outliers from EIA data

    mean = raw_eia_data['value'].mean()
    std = raw_eia_data['value'].std()

    raw_eia_data['value'].apply(process, args=(mean, std), axis = 1)

    vars_ = [
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation", "pressure_msl", "cloud_cover",
        "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
        "et0_fao_evapotranspiration", "vapour_pressure_deficit",
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "sunshine_duration"
    ]

    for var in vars_:
        raw_meteo_data[var].apply(process, args=(mean, std), axis = 1)

    ## Interpolate missing values

    if raw_eia_data.isna().sum() > 0:
        raw_eia_data.interpolate(method = 'linear')

    if raw_meteo_data.isna().sum() > 0:
        raw_meteo_data.interpolate(method = 'linear')

    merge_set = pd.concat(raw_eia_data, raw_meteo_data, axis = 1)
    print(merge_set.describe())


if __file__ == '__main__':
    main()