import os

import modal

image = modal.Image.debian_slim().pip_install("boto3", "pandas", "pyarrow", "numpy", "exchange_calendars", "tqdm")
app = modal.App("bad-apple-backtest", image=image)

WIDTH, HEIGHT = 64, 48
NUM_PIXELS = WIDTH * HEIGHT
DEPLOYED_CAPITAL = 1_000_000.0


@app.function(secrets=[modal.Secret.from_name("bad-apple")], timeout=7200, memory=131072)
def run_backtest(bad_apple_bytes: bytes, assignment_bytes: bytes):
    import json
    import io
    from collections import defaultdict

    import boto3
    import exchange_calendars as xcals
    import numpy as np
    import pandas as pd
    from tqdm import tqdm

    s3 = boto3.client("s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    bucket = os.environ["S3_BUCKET_NAME"]

    splits_raw = json.loads(s3.get_object(Bucket=bucket, Key="config/splits.json")["Body"].read())
    split_cutoffs = {sym: sorted([(pd.Timestamp(d, tz="UTC"), float(f)) for d, f in dates.items()])
                     for sym, dates in splits_raw.items()}

    print("Downloading 15-min BBO from S3...")
    bbo_15min_dfs = []
    for obj in s3.list_objects_v2(Bucket=bucket, Prefix="bbo_15min/").get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            bbo_15min_dfs.append(pd.read_parquet(io.BytesIO(body)))
    bbo_df = pd.concat(bbo_15min_dfs, ignore_index=True)
    bbo_df = bbo_df[bbo_df["spread_bps"] >= 0]
    print(f"Loaded {len(bbo_15min_dfs)} 15-min files")

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
    rebalance_periods = sorted(bbo_df["period"].unique())

    assignment = pd.read_csv(io.BytesIO(assignment_bytes))
    assigned_symbols = set(assignment[assignment["pixel_index"] < NUM_PIXELS]["symbol"])
    sym_to_pixel = dict(zip(assignment["symbol"], assignment["pixel_index"]))

    ohlcv_complete = set(json.loads(s3.get_object(Bucket=bucket, Key="config/ohlcv_complete_symbols.json")["Body"].read()))
    print(f"OHLCV-complete symbols: {len(ohlcv_complete)}")

    period_counts = bbo_df.groupby("symbol")["period"].nunique()
    full_symbols = sorted(s for s in period_counts[period_counts == len(rebalance_periods)].index
                          if s in assigned_symbols and s in ohlcv_complete)
    bbo_df = bbo_df[bbo_df["symbol"].isin(full_symbols)]

    mid_15min = bbo_df.pivot(index="period", columns="symbol", values="mid").sort_index()
    spread_15min = bbo_df.pivot(index="period", columns="symbol", values="spread_bps").sort_index()
    symbols = list(mid_15min.columns)
    sym_to_col = {s: i for i, s in enumerate(symbols)}
    n_symbols = len(symbols)
    del bbo_df

    print("Downloading 1-min BBO from S3...")
    bbo_1min_dfs = []
    for obj in s3.list_objects_v2(Bucket=bucket, Prefix="bbo_1min/").get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            bbo_1min_dfs.append(pd.read_parquet(io.BytesIO(body)))
    bbo_1min_df = pd.concat(bbo_1min_dfs, ignore_index=True)
    bbo_1min_df = bbo_1min_df[bbo_1min_df["symbol"].isin(full_symbols)]
    mid_1min = bbo_1min_df.pivot(index="period", columns="symbol", values="mid").sort_index()
    print(f"Loaded {len(bbo_1min_dfs)} 1-min files")
    del bbo_1min_df, bbo_1min_dfs

    for sym, cutoffs in split_cutoffs.items():
        if sym in mid_15min.columns:
            for cutoff_ts, factor in cutoffs:
                mid_15min.loc[mid_15min.index < cutoff_ts, sym] *= factor
        if sym in mid_1min.columns:
            for cutoff_ts, factor in cutoffs:
                mid_1min.loc[mid_1min.index < cutoff_ts, sym] *= factor

    bad_apple = pd.read_parquet(io.BytesIO(bad_apple_bytes))
    bad_apple["timestamp"] = pd.to_datetime(bad_apple["timestamp"], utc=True)

    common_rebalance = sorted(set(bad_apple["timestamp"]) & set(mid_15min.index))
    all_minutes = sorted(set(mid_1min.index))
    first_day = common_rebalance[0].date()
    last_day = common_rebalance[-1].date()
    valuation_minutes = [ts for ts in all_minutes if first_day <= ts.date() <= last_day]
    n_minutes = len(valuation_minutes)

    mid_15min = mid_15min.loc[common_rebalance].ffill()
    spread_15min = spread_15min.loc[common_rebalance]
    mid_1min = mid_1min.ffill().reindex(valuation_minutes).ffill()
    bad_apple = bad_apple.set_index("timestamp").reindex(common_rebalance)

    print(f"Universe: {n_symbols} symbols, {len(common_rebalance)} rebalances, {n_minutes} valuation minutes")

    pixel_cols = [f"p{i}" for i in range(NUM_PIXELS)]
    pixel_vals = bad_apple[pixel_cols].to_numpy(dtype=np.float32)

    active_mask = pixel_vals > 0
    active_counts_arr = active_mask.sum(axis=1).astype(np.float32)
    active_counts = active_counts_arr.reshape(-1, 1)
    active_counts[active_counts == 0] = 1.0

    norm_pixels = pixel_vals / active_counts
    padded_pixels = np.hstack([norm_pixels, np.zeros((len(common_rebalance), 1), dtype=np.float32)])
    sym_pixel_indices = [sym_to_pixel.get(s, NUM_PIXELS) for s in symbols]
    target_weights_mat = padded_pixels[:, sym_pixel_indices]
    del bad_apple, pixel_vals, norm_pixels, padded_pixels

    dividends_raw = json.loads(s3.get_object(Bucket=bucket, Key="config/dividends_adjusted.json")["Body"].read())
    div_ex_events = defaultdict(list)
    for pay_date, syms in dividends_raw.items():
        for sym, info in syms.items():
            col = sym_to_col.get(sym)
            if col is None:
                continue
            ex_date = pd.Timestamp(info["ex_date"], tz="UTC").date()
            div_ex_events[str(ex_date)].append((col, float(info["amount"]), pay_date))

    rebalance_set = set(common_rebalance)
    rebalance_to_idx = {r: i for i, r in enumerate(common_rebalance)}

    cash = float(DEPLOYED_CAPITAL)
    shares = np.zeros(n_symbols, dtype=np.float64)
    pending_cash_by_date = defaultdict(float)
    current_date_str = None

    mid_15min_arr = mid_15min.to_numpy(dtype=np.float64)
    spread_15min_arr = spread_15min.to_numpy(dtype=np.float64)
    mid_1min_arr = mid_1min.to_numpy(dtype=np.float64)
    del mid_15min, spread_15min, mid_1min

    nav_history = []
    rebalance_history = []
    shares_history = np.zeros((n_minutes, n_symbols), dtype=np.float32)
    values_history = np.zeros((n_minutes, n_symbols), dtype=np.float32)

    print("Running simulation...")
    for t, ts in enumerate(tqdm(valuation_minutes, desc="Minutes")):
        date_str = str(ts.date())

        if date_str != current_date_str:
            if date_str in pending_cash_by_date:
                cash += pending_cash_by_date.pop(date_str)
            if date_str in div_ex_events:
                for sym_idx, amount, pay_date in div_ex_events[date_str]:
                    if shares[sym_idx] > 0:
                        pending_cash_by_date[pay_date] += shares[sym_idx] * amount
            current_date_str = date_str

        if ts in rebalance_set:
            reb_idx = rebalance_to_idx[ts]
            mid = mid_15min_arr[reb_idx]
            half_spread = spread_15min_arr[reb_idx] / 20000.0
            bids = mid * (1.0 - half_spread)
            asks = mid * (1.0 + half_spread)

            pre_positions = float(np.dot(shares, mid))
            pre_liquid_nav = cash + pre_positions
            pre_cash = cash
            pending_val_snapshot = float(sum(pending_cash_by_date.values()))
            pre_nav = pre_liquid_nav + pending_val_snapshot

            w_target = target_weights_mat[reb_idx]
            active_count = int(active_counts_arr[reb_idx])

            if float(w_target.sum()) > 0.0:
                target_shares = np.where(mid > 0, w_target * pre_liquid_nav / mid, 0.0)
                delta = target_shares - shares
                sells = np.maximum(-delta, 0)
                buys = np.maximum(delta, 0)
                sell_proceeds = float(np.dot(sells, bids))
                buy_cost = float(np.dot(buys, asks))
                cash += sell_proceeds
                cash -= buy_cost
                shares = target_shares.copy()
                spread_cost = float(np.dot(sells, mid) - sell_proceeds + buy_cost - np.dot(buys, mid))
            else:
                sell_proceeds = float(np.dot(shares, bids))
                spread_cost = float(np.dot(shares, mid)) - sell_proceeds
                cash += sell_proceeds
                shares = np.zeros(n_symbols, dtype=np.float64)

            post_positions = float(np.dot(shares, mid))
            post_liquid_nav = cash + post_positions
            post_cash = cash
            post_nav = post_liquid_nav + pending_val_snapshot

            rebalance_history.append({
                "period": ts,
                "pre_nav": pre_nav,
                "post_nav": post_nav,
                "pre_liquid_nav": pre_liquid_nav,
                "post_liquid_nav": post_liquid_nav,
                "pre_cash": pre_cash,
                "post_cash": post_cash,
                "pre_positions": pre_positions,
                "post_positions": post_positions,
                "spread_cost": spread_cost,
                "active_count": active_count,
                "pending_dividends": pending_val_snapshot,
            })

        val_mid = mid_1min_arr[t]
        position_values = shares * val_mid
        positions_val = float(np.nansum(position_values))
        pending_val = float(sum(pending_cash_by_date.values()))

        shares_history[t] = shares.astype(np.float32)
        values_history[t] = np.nan_to_num(position_values, nan=0.0).astype(np.float32)

        nav_history.append({
            "period": ts,
            "nav": cash + positions_val + pending_val,
            "liquid_nav": cash + positions_val,
            "cash": cash,
            "positions": positions_val,
            "pending_dividends": pending_val,
        })

    final = nav_history[-1]
    print(f"\n[{final['period']}] NAV=${final['nav']:,.0f} (FINAL)")
    print(f"Return: {(final['nav'] / nav_history[0]['nav'] - 1) * 100:.2f}%")

    print("Saving results...")
    nav_df = pd.DataFrame(nav_history)
    buf = io.BytesIO()
    nav_df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key="results/backtest_nav.parquet", Body=buf.getvalue())
    print("Uploaded results/backtest_nav.parquet")

    rebalance_df = pd.DataFrame(rebalance_history)
    buf = io.BytesIO()
    rebalance_df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key="results/backtest_rebalances.parquet", Body=buf.getvalue())
    print("Uploaded results/backtest_rebalances.parquet")

    shares_df = pd.DataFrame(shares_history, index=valuation_minutes, columns=symbols)
    buf = io.BytesIO()
    shares_df.to_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key="results/backtest_shares.parquet", Body=buf.getvalue())
    print("Uploaded results/backtest_shares.parquet")

    values_df = pd.DataFrame(values_history, index=valuation_minutes, columns=symbols)
    buf = io.BytesIO()
    values_df.to_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key="results/backtest_values.parquet", Body=buf.getvalue())
    print("Uploaded results/backtest_values.parquet")

    return nav_df.to_parquet()


@app.local_entrypoint()
def main():
    from config import DATA_DIR, get_s3_client, get_s3_bucket

    s3 = get_s3_client()
    bucket = get_s3_bucket()

    bad_apple_bytes = (DATA_DIR / "bad_apple_frames.parquet").read_bytes()
    assignment_bytes = (DATA_DIR / "ticker_assignment.csv").read_bytes()

    print("Dispatching backtest to Modal...")
    result_bytes = run_backtest.remote(bad_apple_bytes, assignment_bytes)

    out_path = DATA_DIR / "backtest_nav.parquet"
    out_path.write_bytes(result_bytes)
    print(f"Downloaded NAV results to {out_path}")

    s3.download_file(bucket, "results/backtest_rebalances.parquet", str(DATA_DIR / "backtest_rebalances.parquet"))
    print(f"Downloaded rebalances to {DATA_DIR / 'backtest_rebalances.parquet'}")

    s3.download_file(bucket, "results/backtest_shares.parquet", str(DATA_DIR / "backtest_shares.parquet"))
    print(f"Downloaded shares to {DATA_DIR / 'backtest_shares.parquet'}")

    s3.download_file(bucket, "results/backtest_values.parquet", str(DATA_DIR / "backtest_values.parquet"))
    print(f"Downloaded values to {DATA_DIR / 'backtest_values.parquet'}")
