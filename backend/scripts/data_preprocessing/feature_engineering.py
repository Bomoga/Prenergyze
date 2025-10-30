from __future__ import annotations

import argparse
from math import pi
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

## lowk i forgot that using slashes is easier than using os.join
BACKEND_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BACKEND_DIR / "data" / "processed"
DEFAULT_INPUT = DATA_DIR / "CLEAN_MERGED_DATASET.csv"
DEFAULT_OUTPUT = DATA_DIR / "FEATURE_ENGINEERED_DATASET.csv"

## Set up long load lags and roll windows only for specific features bc we dont want an exorbitant amount of needless engineered features
TARGET = "load"
LOAD_LAGS: Sequence[int] = (1, 2, 3, 24, 25, 168)
LOAD_ROLL_WINDOWS: Sequence[int] = (3, 24, 168)
THERMAL_COLS: Sequence[str] = (
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "vapour_pressure_deficit",
    "pressure_msl",
)

## Same as above except with lower lag and roll windows because theyre less impactful
THERMAL_LAGS: Sequence[int] = (1, 24)
THERMAL_ROLL_WINDOWS: Sequence[int] = (3, 24)
SHORT_MEMORY_COLS: Sequence[str] = (
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "et0_fao_evapotranspiration",
    "sunshine_duration",
    "wind_speed_10m",
    "wind_gusts_10m",
)
PRECIP_COL = "precipitation"
CORR_THRESHOLD = 0.98
MAX_FEATURES = 200
TIME_FEATURES = ("hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos")
WIND_FEATURE_PREFIXES = ("wind_dir_sin_10m", "wind_dir_cos_10m")
BASE_FEATURE_ORDER = (
    *(col for col in THERMAL_COLS),
    PRECIP_COL,
    *(col for col in SHORT_MEMORY_COLS),
)

## Dataset loader helper func that removes the starting unnamed index column aswell
def load_dataset(path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["date"])
    unnamed = [col for col in df.columns if col.lower().startswith("unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed)
    df = df.sort_values("date").drop_duplicates(subset="date")
    return df.set_index("date")

## Makes sure all values in the dataframe are numeric so that the mean, std, etc. calculations don't error out
def to_numeric(df, columns) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

## For each lag value in the previously set up lag lists (i.e. (1, 2, 3, 24, 25, 168)), create new columns and calculate for each using df.shift
def lag_features(series, name, lags) -> pd.DataFrame:
    return pd.DataFrame({f"{name}_lag_{lag}h": series.shift(lag) for lag in lags}, index=series.index)

## For each roll window in the previously set up roll window lists (i.e. (3, 24, 168)), create a new rolling window 
## and create a new column for each specified stat (i.e. "mean", "std").
def roll_features(
    series: pd.Series,
    name: str,
    windows: Sequence[int],
    stats: Sequence[str],
) -> pd.DataFrame:
    shifted = series.shift(1)
    data: dict[str, pd.Series] = {}
    for window in windows:
        roll = shifted.rolling(window=window, min_periods=window)
        for stat in stats:
            if stat == "mean":
                values = roll.mean()
            elif stat == "std":
                values = roll.std()
            elif stat == "sum":
                values = roll.sum()
            else:
                raise ValueError(f"Unsupported metric: {stat}")
            data[f"{name}_roll_{stat}_{window}h"] = values
    return pd.DataFrame(data, index=series.index)

## Turns the datetime column values into a clock format by projecting the values onto a unit circle.
## Asked chatgpt and apparently it helps the model understand which times are next to which (like 23:00 is next to 00:00)
## Pretty cool stuff. decided to include it
def time_features(index: pd.DatetimeIndex) -> pd.DataFrame:
    hours = index.hour + index.minute / 60
    dow = index.dayofweek
    month = index.month
    return pd.DataFrame(
        {
            "hour_sin": np.sin(2 * pi * hours / 24),
            "hour_cos": np.cos(2 * pi * hours / 24),
            "dow_sin": np.sin(2 * pi * dow / 7),
            "dow_cos": np.cos(2 * pi * dow / 7),
            "month_sin": np.sin(2 * pi * (month - 1) / 12),
            "month_cos": np.cos(2 * pi * (month - 1) / 12),
        },
        index=index,
    )

## Converted wind direction to radians and computed cos and sin components so that the model doesnt have to worry about
## degree wrap-around problems (0 degrees = 360 degrees)
def wind_direction_features(df: pd.DataFrame) -> pd.DataFrame:
    if "wind_direction_10m" not in df.columns:
        return pd.DataFrame(index=df.index)
    radians = np.deg2rad(df["wind_direction_10m"])
    sin_series = np.sin(radians)
    cos_series = np.cos(radians)
    base = pd.DataFrame(
        {"wind_dir_sin_10m": sin_series, "wind_dir_cos_10m": cos_series},
        index=df.index,
    )
    smooth = pd.concat(
        [
            roll_features(pd.Series(sin_series, index=df.index), "wind_dir_sin_10m", (3,), ("mean",)),
            roll_features(pd.Series(cos_series, index=df.index), "wind_dir_cos_10m", (3,), ("mean",)),
        ],
        axis=1,
    )
    return pd.concat([base, smooth], axis=1)

## Removes potential constant columns (useless)
def drop_constant(df):
    keep = [col for col in df.columns if df[col].nunique(dropna=True) > 1]
    return df[keep]

## Removes almost constant columns (columns that barely change at all such as within 0.001)
def drop_quasi_constant(df, threshold: float = 0.995):
    keep = []
    for col in df.columns:
        counts = df[col].value_counts(normalize=True, dropna=True)
        if counts.empty or counts.iloc[0] < threshold:
            keep.append(col)
    return df[keep]

## Drop any potential engineered duplicates by transposing the features as rows and locating identical rows
def drop_duplicates(df):
    dup_mask = df.T.duplicated()
    return df.loc[:, ~dup_mask]

## Creates an absolute correlation matrix and only keeps columns whose absolute corr with every other feature
## that is currently already kept is <= 0.98
def drop_high_corr(df, target, threshold):
    features = [col for col in df.columns if col != target]
    if len(features) < 2:
        return df
    corr = df[features].corr().abs()
    selected: list[str] = []
    for col in sorted(features, key=lambda x: (0 if x.startswith(f"{target}_") else 1, len(x), x)):
        if all(corr.loc[col, kept] <= threshold for kept in selected):
            selected.append(col)
    cols = [target] + selected
    return df[cols]

## Prevents the feature list from ever exceeding 200 to prevent dataset bloating
def enforce_limit(df: pd.DataFrame, target: str, max_cols: int) -> pd.DataFrame:
    if df.shape[1] <= max_cols:
        return df
    base = [target]
    others = [c for c in df.columns if c != target]
    ordered = sorted(others, key=lambda x: (0 if x.startswith(f"{target}_") else 1, len(x), x))
    keep = base + ordered[: max_cols - 1]
    return df[keep]

## Organizes the columns. Idk how I havent analysed it. ChatGPT generated this. It works well enough tho
def organize_columns(df: pd.DataFrame) -> pd.DataFrame:
    def priority(col: str) -> tuple[int, int, str]:
        if col == TARGET:
            return (0, 0, col)
        if col in BASE_FEATURE_ORDER:
            return (1, BASE_FEATURE_ORDER.index(col), col)
        if col in TIME_FEATURES:
            return (2, TIME_FEATURES.index(col), col)
        if any(col.startswith(prefix) for prefix in WIND_FEATURE_PREFIXES):
            return (3, len(col), col)
        if col.startswith(f"{TARGET}_lag_"):
            return (4, len(col), col)
        if col.startswith(f"{TARGET}_roll_"):
            return (5, len(col), col)
        if any(col.startswith(f"{feature}_lag_") for feature in THERMAL_COLS):
            return (6, len(col), col)
        if any(col.startswith(f"{feature}_roll_") for feature in THERMAL_COLS):
            return (7, len(col), col)
        if any(col.startswith(feature) for feature in SHORT_MEMORY_COLS):
            return (8, len(col), col)
        if col.startswith(f"{PRECIP_COL}_"):
            return (9, len(col), col)
        return (10, len(col), col)

    ordered_cols = sorted(df.columns, key=priority)
    return df[ordered_cols]

## Main area where all functions are pieced together to engineer the dataset
def engineer(df):
    to_numeric(df, df.columns)

    pieces = [df.drop(columns=["wind_direction_10m"], errors="ignore")]
    pieces.append(time_features(df.index))
    pieces.append(wind_direction_features(df))

    if TARGET in df.columns:
        target_series = df[TARGET]
        pieces.append(lag_features(target_series, TARGET, LOAD_LAGS))
        pieces.append(roll_features(target_series, TARGET, LOAD_ROLL_WINDOWS, ("mean", "std")))

    for col in THERMAL_COLS:
        if col not in df.columns:
            continue
        pieces.append(lag_features(df[col], col, THERMAL_LAGS))
        pieces.append(roll_features(df[col], col, THERMAL_ROLL_WINDOWS, ("mean",)))

    for col in SHORT_MEMORY_COLS:
        if col not in df.columns:
            continue
        pieces.append(roll_features(df[col], col, (3,), ("mean",)))

    if PRECIP_COL in df.columns:
        pieces.append(roll_features(df[PRECIP_COL], PRECIP_COL, (3,), ("sum",)))

    feature_df = pd.concat(pieces, axis=1)
    feature_df = drop_constant(feature_df)
    feature_df = drop_quasi_constant(feature_df)
    feature_df = drop_duplicates(feature_df)
    feature_df = drop_high_corr(feature_df, target=TARGET, threshold=CORR_THRESHOLD)
    feature_df = enforce_limit(feature_df, target=TARGET, max_cols=MAX_FEATURES)
    feature_df = organize_columns(feature_df)
    return feature_df

def main():
    df = load_dataset(DEFAULT_INPUT)
    features = engineer(df)
    features.reset_index().to_csv(DEFAULT_OUTPUT, index=False)

if __name__ == "__main__":
    main()
