import json

import exchange_calendars as xcals
import numpy as np
import pandas as pd
from numba import njit, prange
from scipy.optimize import linear_sum_assignment

from config import DATA_DIR, NUM_PIXELS, WIDTH, HEIGHT, get_s3_client, get_s3_bucket


BBO_15MIN_DIR = DATA_DIR / "bbo_15min"
FORCED_ASSIGNMENTS = {
    "AAPL": 0,   "NVDA": 1,   "TSLA": 2,
    "MSFT": 64,  "AMZN": 65,  "GOOGL": 66,
    "META": 128, "NFLX": 129, "AMD": 130,
}

s3 = get_s3_client()
bucket = get_s3_bucket()

splits_raw = json.loads(s3.get_object(Bucket=bucket, Key="config/splits.json")["Body"].read())
split_cutoffs = {sym: sorted([(pd.Timestamp(d, tz="UTC"), float(f)) for d, f in dates.items()])
                 for sym, dates in splits_raw.items()}

print("Loading BBO data...")
bbo_dfs = [pd.read_parquet(f) for f in sorted(BBO_15MIN_DIR.glob("*.parquet"))]
bbo_df = pd.concat(bbo_dfs, ignore_index=True)
bbo_df = bbo_df[bbo_df["spread_bps"] >= 0]

xnas = xcals.get_calendar("XNAS")
valid_periods = set()
for date in bbo_df["period"].dt.date.unique():
    sched = xnas.schedule.loc[str(date):str(date)]
    if sched.empty:
        continue
    t = sched.iloc[0]["open"] + pd.Timedelta(minutes=15)
    end = sched.iloc[0]["close"] - pd.Timedelta(minutes=15)
    while t <= end:
        valid_periods.add(t)
        t += pd.Timedelta(minutes=15)

bbo_df = bbo_df[bbo_df["period"].isin(valid_periods)]
periods = sorted(bbo_df["period"].unique())
print(f"Total periods: {len(periods)}")

lseg_covered = pd.read_csv(DATA_DIR / "lseg_covered_symbols.csv")
lseg_symbols = set(lseg_covered["symbol"])
print(f"LSEG-covered symbols: {len(lseg_symbols)}")

ohlcv_complete_raw = json.loads(s3.get_object(Bucket=bucket, Key="config/ohlcv_complete_symbols.json")["Body"].read())
ohlcv_complete_symbols = set(ohlcv_complete_raw)
print(f"OHLCV-complete symbols: {len(ohlcv_complete_symbols)}")

period_counts = bbo_df.groupby("symbol")["period"].nunique()
full_symbols = sorted(s for s in period_counts[period_counts == len(periods)].index
                      if s in lseg_symbols and s in ohlcv_complete_symbols)
bbo_df = bbo_df[bbo_df["symbol"].isin(full_symbols)]

spreads_df = bbo_df.pivot(index="period", columns="symbol", values="spread_bps").sort_index().reindex(periods)
mid_df = bbo_df.pivot(index="period", columns="symbol", values="mid").sort_index().reindex(periods)
symbols = list(spreads_df.columns)
symbol_to_col = {s: i for i, s in enumerate(symbols)}
N = len(symbols)
print(f"Universe: {N} symbols")

for sym, cutoffs in split_cutoffs.items():
    if sym not in mid_df.columns:
        continue
    for cutoff_ts, factor in cutoffs:
        mid_df.loc[mid_df.index < cutoff_ts, sym] *= factor

price_matrix = mid_df.ffill().to_numpy(dtype=np.float32)
print("Price matrix built from BBO mid prices.")

dividends_raw = json.loads(s3.get_object(Bucket=bucket, Key="config/dividends_adjusted.json")["Body"].read())
div_matrix = np.zeros((len(periods), N), dtype=np.float32)
period_dates = [p.date() for p in periods]
date_to_first_idx = {}
for i, d in enumerate(period_dates):
    if d not in date_to_first_idx:
        date_to_first_idx[d] = i

for pay_date, syms in dividends_raw.items():
    for sym, info in syms.items():
        j = symbol_to_col.get(sym)
        if j is None:
            continue
        ex_date = pd.Timestamp(info["ex_date"], tz="UTC").date()
        idx = date_to_first_idx.get(ex_date)
        if idx is not None:
            div_matrix[idx, j] = float(info["amount"])

returns = np.zeros_like(price_matrix, dtype=np.float32)
with np.errstate(divide="ignore", invalid="ignore"):
    returns[1:] = (price_matrix[1:] + div_matrix[1:]) / price_matrix[:-1] - 1.0
returns = np.nan_to_num(returns, nan=0.0, posinf=0.0, neginf=0.0)

bad_apple = pd.read_parquet(DATA_DIR / "bad_apple_frames.parquet")
bad_apple["timestamp"] = pd.to_datetime(bad_apple["timestamp"], utc=True)

narrative_file = DATA_DIR / "bad_apple_narrative.parquet"
narrative_df = pd.read_parquet(narrative_file) if narrative_file.exists() else None
if narrative_df is not None:
    narrative_df["timestamp"] = pd.to_datetime(narrative_df["timestamp"], utc=True)

common_periods = sorted(set(bad_apple["timestamp"]) & set(periods))
print(f"Common simulation periods: {len(common_periods)}")

period_to_idx = {p: i for i, p in enumerate(periods)}
idx_list = [period_to_idx[p] for p in common_periods]
returns = returns[idx_list]
spreads = spreads_df.loc[common_periods].values.astype(np.float32)
bad_apple = bad_apple[bad_apple["timestamp"].isin(common_periods)].sort_values("timestamp").reset_index(drop=True)

if narrative_df is not None:
    narrative_df = narrative_df[narrative_df["timestamp"].isin(common_periods)].sort_values("timestamp").reset_index(drop=True)
    s_k = narrative_df["s"].to_numpy(dtype=np.float32).reshape(-1, 1)
else:
    s_k = np.ones((len(common_periods), 1), dtype=np.float32)

pixel_cols = [f"p{i}" for i in range(NUM_PIXELS)]
pixel_vals = bad_apple[pixel_cols].to_numpy(dtype=np.float32)

active_mask = pixel_vals > 0
active_counts = active_mask.sum(axis=1, keepdims=True).astype(np.float32)
active_counts[active_counts == 0] = 1.0
weights = pixel_vals / active_counts

w_prev, w_curr = weights[:-1], weights[1:]
r_curr = returns[1:]
k_curr = (spreads[1:] / 10000.0 * 0.5).astype(np.float32)
s_curr = s_k[1:]

# If you try computing the utility matrix at once with broadcasting, then you
# will run out of memory unless you genuinely have a supercomputer. It'd take
# like 420 GB to store the entire intermediate tensor in memory. So we split up
# the computation into pieces. We can compute the gross returns using vectorized
# operations, but we have to iterate through time to compute the tcosts. I've
# wrapped the iteration in numba so that it's not too painfully slow.
print(f"Computing returns matrix...")
sg_prev = (s_curr * w_prev).astype(np.float32)
gross_matrix = (r_curr.T @ sg_prev).astype(np.float32)


print("Computing cost matrix...")
r_plus_1 = (1.0 + r_curr).astype(np.float32)

@njit(parallel=True)
def compute_cost_matrix(w_prev, w_curr, r_plus_1, k_curr):
    T, NUM_PIXELS = w_prev.shape
    N = r_plus_1.shape[1]
    cost = np.zeros((N, NUM_PIXELS), dtype=np.float32)
    for i in prange(NUM_PIXELS):
        for j in range(N):
            c = 0.0
            for k in range(T):
                drifted = w_prev[k, i] * r_plus_1[k, j]
                c += abs(w_curr[k, i] - drifted) * k_curr[k, j]
            cost[j, i] = c
    return cost

cost_matrix = compute_cost_matrix(w_prev, w_curr, r_plus_1, k_curr)

utility_matrix = gross_matrix - cost_matrix

forced_sym_set = {s for s in FORCED_ASSIGNMENTS if s in symbol_to_col}
forced_pix_set = {FORCED_ASSIGNMENTS[s] for s in forced_sym_set}
opt_sym_idx = [i for i, s in enumerate(symbols) if s not in forced_sym_set]
opt_pix_idx = [i for i in range(NUM_PIXELS) if i not in forced_pix_set]

print(f"Solving assignment ({len(opt_sym_idx)} symbols x {len(opt_pix_idx)} pixels)...")
row_ind, col_ind = linear_sum_assignment(-utility_matrix[np.ix_(opt_sym_idx, opt_pix_idx)])
assigned_map = {opt_sym_idx[r]: opt_pix_idx[c] for r, c in zip(row_ind, col_ind)}
for sym, pix in FORCED_ASSIGNMENTS.items():
    if sym in symbol_to_col:
        assigned_map[symbol_to_col[sym]] = pix

results = []
dummy_counter = NUM_PIXELS
for row in range(N):
    sym = symbols[row]
    if row in assigned_map:
        results.append({"symbol": sym, "pixel_index": int(assigned_map[row])})
    else:
        results.append({"symbol": sym, "pixel_index": dummy_counter})
        dummy_counter += 1

out_path = DATA_DIR / "ticker_assignment.csv"
pd.DataFrame(results).to_csv(out_path, index=False)
print(f"Saved assignment to {out_path}")
