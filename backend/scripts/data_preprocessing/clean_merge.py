import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path
import numpy as np

BASE_DIR = Path(os.getcwd())
raw_eia_output_path = Path(os.path.join(f"{BASE_DIR}","backend", "data", "raw", "eia", "FPL_DEMAND_2023-09-01T00_2025-09-01T00.csv"))
raw_meteo_output_path = Path(os.path.join(f"{BASE_DIR}", "backend", "data", "raw", "meteo", "METEO_28.084358_-82.372894_2023-09-01_2025-09-01.csv"))
 
def main():
    ## Read CSVs from raw folder

    raw_eia_data = pd.read_csv(raw_eia_output_path)
    raw_meteo_data = pd.read_csv(raw_meteo_output_path).reset_index(drop=True)
    
    ## Drop meaningless columns

    raw_eia_data = raw_eia_data.drop(columns = ["respondent","respondent-name","type","type-name","value-units"])
    raw_meteo_data = raw_meteo_data.drop(columns = ['date'])

    ## Rename vague column titles

    raw_eia_data = raw_eia_data.rename(columns = {'period':'date', 'value': 'load'})

    ## Drop null vals from both datasets

    raw_eia_data.dropna()
    raw_meteo_data.dropna()

    ## Drop duplicates

    raw_eia_data['date'].drop_duplicates()

    ## Remove outliers from EIA data using Z-scores

    mean = raw_eia_data['load'].mean()
    std = raw_eia_data['load'].std()

    z = (raw_eia_data['load'] - mean) / std
    raw_eia_data.loc[z.abs() > 3, 'load'] = np.nan

    vars_ = [
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation", "pressure_msl", "cloud_cover",
        "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
        "et0_fao_evapotranspiration", "vapour_pressure_deficit",
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "sunshine_duration"
    ]

    for var in vars_:
        col_mean = raw_meteo_data[var].mean()
        col_std = raw_meteo_data[var].std()
        z = (raw_meteo_data[var] - col_mean) / col_std
        raw_meteo_data.loc[z.abs() > 3, var] = np.nan

    ## Interpolate missing values

    if  raw_eia_data['load'].isna().sum() > 0:
        raw_eia_data.interpolate(method = 'linear')

    for var in vars_:
        if raw_meteo_data[var].isna().sum() > 0:
            raw_meteo_data[var].interpolate(method = 'linear')

    ## Merge both cleaned datasets

    merged_set = pd.concat([raw_eia_data, raw_meteo_data], axis = 1)

    ## Export to processed folder

    output_path = Path(os.path.join(f"{BASE_DIR}", "backend", "data", "processed", f"CLEAN_MERGED_DATASET.csv"))
    merged_set.to_csv(output_path)

if __name__ == '__main__':
    main()