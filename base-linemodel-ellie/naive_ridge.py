'''
Developer: Ellie Khanh Truong
'''
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV
import matplotlib.pyplot as plt
from sklearn.linear_model import Ridge
import seaborn as sns
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error
from scipy.stats import loguniform


df = pd.read_csv(
    '/workspaces/Prenergyze/backend/data/processed/FEATURE_ENGINEERED_DATASET.csv')
df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]
# make a file to save all the visualization to find the correlations between features
plot_dir = Path('/workspaces/Prenergyze/backend/plots')
plot_dir.mkdir(parents=True, exist_ok=True)
print(df.head())
print(df.describe())
print(df.info())
print(df.isna().sum())
print(df.columns)
print(df['relative_humidity_2m'].describe())


df['date'] = pd.to_datetime(df['date'])
df.sort_values(by='date')
df = df.sort_values('date').reset_index(drop=True)
df['target'] = df['load'].shift(-1)  # move the value down 1 row

# drop the last line, because the target gna be NaN after shift
df = df.dropna(subset=['target']).reset_index(drop=True)
feature_cols = [c for c in df.columns if c not in {'date', 'load', 'target'}]
X = df[feature_cols].copy()
Y = df['target'].copy()
print("X shape", X.shape)
print("Y shape", Y.shape)
print("First features:", list(X.columns[:5]))
print("First 5 y values:", Y.head())

# 73 days of hourly data, split in 10 set of data
splitter = TimeSeriesSplit(
    n_splits=5, max_train_size=None, test_size=1752, gap=2)

for fold, (train_idx, test_idx) in enumerate(splitter.split(X)):
    print("TRAIN:", train_idx, "TEST:", test_idx)
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    Y_train, Y_test = Y.iloc[train_idx], Y.iloc[test_idx]

plt.figure(figsize=(12, 5))
plt.plot(df['date'], df['load'], label='Load', linewidth=1)
plt.title("Electric Load over Time")
plt.xlabel("Date")
plt.ylabel("Load (NW)")
plt.legend()
out1 = plot_dir/'load_over_time.png'
plt.savefig(out1, dpi=150)
plt.close()

# weather panels(only if the columns exists)
for col in ['temperature_2m', 'relative_humidity_2m', 'precipitation']:
    if col not in df.columns:
        df[col] = pd.NA  # avoid KeyError
# share the same data scales
fig, ax = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
ax[0].plot(df['date'], df['temperature_2m'], color='orange')
ax[0].set_title("Temp over time")

ax[1].plot(df['date'], df['relative_humidity_2m'], color='pink')
ax[1].set_title("Humid over time")

ax[2].plot(df['date'], df['precipitation'], color='blue')
ax[2].set_title('Precipitation over time')

plt.xlabel("Date")
plt.tight_layout()
out2 = plot_dir / 'weather_panels.png'
fig.savefig(out2, dpi=150)
plt.close(fig)
print(f"Saved: {out2}")


# heatmap the see the correlation with load
# for readability
num = df.select_dtypes('number')
if 'load' in num.columns and num.shape[1] >= 2:
    top_cols = num.corr(numeric_only=True)['load'].abs(
    ).sort_values(ascending=False).head(12).index
    plt.figure(figsize=(10, 8))
    sns.heatmap(num[top_cols].corr(), cmap='coolwarm',
                annot=True, vmin=-1, vmax=1)
    plt.title("Top correlation features")
    plt.tight_layout()
    plt.savefig(plot_dir/'corr_heatmap_top.png', dpi=300)
    plt.close()

pair_cols = ['load', 'temperature_2m', 'relative_humidity_2m', 'precipitation']
use = df[pair_cols].dropna()
if len(use) > 5000:  # make sure the data is not lagging
    use = use.sample(5000, random_state=42)
g = sns.pairplot(use, corner=True, plot_kws={'alpha': 0.3, 's': 8})
g.fig.suptitle("Scatter Matrix (sampled)", y=1.02)
g.savefig(plot_dir/'pairplot_sampled.png', dpi=300, bbox_inches='tight')
plt.close()


# visualize the temperature correlate with the target "load"
fig, axs = plt.subplots(1, 3, figsize=(18, 5))
sns.scatterplot(x='temperature_2m', y='load', data=df, alpha=0.3, ax=axs[0])
axs[0].set_title("Load vs Temperature (Current)")

sns.scatterplot(x='temperature_2m_lag_24h', y='load',
                data=df, alpha=0.3, ax=axs[1])
axs[1].set_title("Load vs Temperature (24h lag)")

sns.scatterplot(x='temperature_2m_roll_mean_24h',
                y='load', data=df, alpha=0.3, ax=axs[2])
axs[2].set_title("Load vs Temperature (24h rolling mean)")
out_path = plot_dir/"load_vs_temp_all.png"
fig.savefig(out_path, dpi=300)
plt.close(fig)


# visualize the humidity correlate with the target "load"
cols = [("relative_humidity_2m", "Load vs Humidity (Current)"),
        ("relative_humidity_2m_lag_24h", "Load vs Humidity (24h Lag)"),
        ("relative_humidity_2m_roll_mean_24h",
         "Load vs Humidity (24h Rolling Mean)")
        ]

# define the present before subsplot, only keep the data thats in the columns
present = [(c, t) for c, t in cols if c in df.columns]

if not present:
    raise ValueError("No humidity columns found in df plot")

fig, axs = plt.subplots(1, len(present), figsize=(6*len(present), 5))
if len(present) == 1:
    axs = [axs]

for ax, (col, title) in zip(axs, present):
    sns.scatterplot(data=df, x=col, y='load', alpha=0.3, s=12, ax=ax)
    ax.set_title(title)
    ax.set_xlabel("Relative Humidity (%)")
    ax.set_ylabel("Electric Load (MW)")
plt.tight_layout()
out_path2 = plot_dir/"load_vs_humidity_all.png"
fig.savefig(out_path2, dpi=300)
plt.close(fig)
# the correlation is weak and wobbly

# visualize for the precipitation
# using the roll 3h: total rainfall over the past 3 hours
fig, axs = plt.subplots(1, 2, figsize=(18, 5))
sns.scatterplot(x='precipitation', y='load', data=df, alpha=0.3, ax=axs[0])
axs[0].set_title("Current Precipitation")

sns.scatterplot(x='precipitation_roll_sum_3h',
                y='load', data=df, alpha=0.3, ax=axs[1])
axs[1].set_title("3h Rolling Sum")

plt.tight_layout()
out_path = plot_dir / "load_vs_precip_all.png"
fig.savefig(out_path, dpi=300)
plt.close(fig)

# shrink all the heavy tails data first
features_all = ['load_lag_1h', 'load_lag_2h', 'load_lag_3h', 'load_lag_24h', 'load_lag_168h',
                'temperature_2m', 'temperature_2m_lag_24h', 'temperature_2m_roll_mean_24h',
                'apparent_temperature', 'apparent_temperature_lag_24h',
                'relative_humidity_2m', 'pressure_msl',
                'precipitation', 'precipitation_roll_sum_3h',
                'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos', 'month_sin', 'month_cos'
                ]
features = [c for c in features_all if c in df.columns]
missing = sorted(set(features_all)-set(features))
if missing:
    print("Emergency I LOST MY FEATURE")
X = df[features].copy()
Y = df['target'].copy()

print(df[features].skew(numeric_only=True).sort_values(ascending=False))

# u see the precipitation and roll sum 3h fucked up -> right skewed
skewed_candidates = ['precipitation', 'precipitation_roll_sum_3h']
skewed_features = [c for c in skewed_candidates if c in features]
normal_features = [c for c in features if c not in skewed_features]

# scaling the data
transformers = []
if skewed_features:
    skewed_pipe = Pipeline([
        ('log1p', FunctionTransformer(np.log1p, validate=False)),
        ('scale', StandardScaler())
    ])
    transformers.append(('skewed', skewed_pipe, skewed_features))
if normal_features:
    transformers.append(('normal', StandardScaler(), normal_features))

if not transformers:
    raise ValueError("We cooked! No usable features")
# build the pipeline for skewed features bro
pre = ColumnTransformer(transformers, remainder='drop')
# FINALLY TRAIN RIDGE
ridge_reg = Ridge(alpha=0.1, solver='cholesky')
ridge_pipe = Pipeline([
    ('prep', pre),
    ('ridge', ridge_reg)
])

ridge_search = RandomizedSearchCV(
    ridge_pipe, param_distributions={'ridge__alpha': loguniform(1e-3, 1e3)},
    n_iter=25, cv=splitter,
    scoring='neg_root_mean_squared_error',
    n_jobs=-1,
    random_state=42,
    verbose=1
)
ridge_search.fit(X, Y)
ridge_search_rmse = -ridge_search.best_score_
print(ridge_search_rmse)
print(ridge_search.best_params_)

# try Naive baseline model just in case


def rmse(a, b):
    return np.sqrt(mean_squared_error(a, b))


naive_rmse = []
for fold, (tr, te) in enumerate(splitter.split(X)):
    y_test = Y.iloc[te]
    y_pred_naive = df.loc[y_test.index, 'load']
    r = rmse(y_test, y_pred_naive)
    naive_rmse.append(r)
    print(f"Fold {fold+1} Naive RMSE: {r:.2f}")

print("Average Naive RMSE:", np.mean(naive_rmse))
