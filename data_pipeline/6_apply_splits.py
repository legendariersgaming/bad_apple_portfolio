import json

import databento as db
import exchange_calendars as xcals
import numpy as np
import pandas as pd

from config import get_s3_client, get_s3_bucket

s3 = get_s3_client()
bucket = get_s3_bucket()
xnas = xcals.get_calendar("XNAS")

ohlcv_dfs = []
symbology = {}
for obj in s3.list_objects_v2(Bucket=bucket, Prefix="ohlcv/").get("Contents", []):
    key = obj["Key"]
    if key.endswith(".ohlcv-1d.dbn.zst"):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        with open("/tmp/ohlcv.dbn.zst", "wb") as f:
            f.write(body)
        df = db.DBNStore.from_file("/tmp/ohlcv.dbn.zst").to_df().reset_index()
        ohlcv_dfs.append(df)
    elif "symbology_" in key and key.endswith(".json"):
        sym_data = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        for symbol, entries in sym_data.get("result", {}).items():
            for entry in entries:
                symbology[str(entry["s"])] = symbol

if not ohlcv_dfs:
    raise ValueError("No OHLCV-1d data found in S3. OHLCV-1d data is required for split detection.")
else:
    ohlcv_df = pd.concat(ohlcv_dfs, ignore_index=True)
    ohlcv_df["symbol"] = ohlcv_df["instrument_id"].astype(str).map(symbology)
    ohlcv_df = ohlcv_df.dropna(subset=["symbol"])
    ohlcv_df["date"] = pd.to_datetime(ohlcv_df["ts_event"]).dt.strftime("%Y-%m-%d")
    daily_closes = ohlcv_df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
    daily_opens = ohlcv_df.pivot_table(index="date", columns="symbol", values="open", aggfunc="first")
    dates_sorted = sorted(daily_closes.index.tolist())
    print(f"Loaded {len(dates_sorted)} days of OHLCV data from S3 for {len(daily_closes.columns)} symbols")

    ohlcv_counts = ohlcv_df.groupby("symbol")["date"].nunique()
    ohlcv_complete_symbols = sorted(ohlcv_counts[ohlcv_counts == len(dates_sorted)].index.tolist())
    print(f"Symbols with complete OHLCV data: {len(ohlcv_complete_symbols)} / {len(daily_closes.columns)}")

def first_trading_day_on_or_after(date_str):
    sched = xnas.schedule.loc[date_str:]
    return str(sched.index[0].date()) if len(sched) > 0 else None

# This wouldn't be necessary if I could figure out how the hell to find
# the CORRECT application date for adjustment factors
def find_split_date(sym, lseg_date_str, factor):
    start_date = first_trading_day_on_or_after(lseg_date_str)
    if not start_date or start_date not in dates_sorted:
        return None
    start_idx = dates_sorted.index(start_date)
    # Use extended window only for large splits (factor > 2 or < 0.5)
    # Small factors are usually accurately dated and risk false matches
    search_days = 30 if (factor > 2 or factor < 0.5) else 4
    for offset in range(search_days):
        candidate_idx = start_idx + offset
        if candidate_idx >= len(dates_sorted) or candidate_idx < 1:
            continue
        prev_date = dates_sorted[candidate_idx - 1]
        curr_date = dates_sorted[candidate_idx]
        prev_price = daily_closes.loc[prev_date, sym] if sym in daily_closes.columns else None
        curr_price = daily_opens.loc[curr_date, sym] if sym in daily_opens.columns else None
        if prev_price and curr_price and prev_price > 0 and not np.isnan(prev_price) and not np.isnan(curr_price):
            ratio = curr_price / prev_price
            if abs(np.log(ratio) - np.log(factor)) < 0.2:
                return curr_date
    return None

splits_lseg = json.loads(s3.get_object(Bucket=bucket, Key="config/splits_lseg.json")["Body"].read())
print(f"Loaded {len(splits_lseg)} symbols with splits from LSEG")

ohlcv_start = dates_sorted[0]
ohlcv_end = dates_sorted[-1]

splits = {}
dropped_symbols = []

for sym, split_list in splits_lseg.items():
    sym_splits = {}
    sym_dropped = False
    for split_info in split_list:
        date_str = split_info["date"]
        factor = split_info["factor"]
        date_type = split_info["date_type"]

        if date_str < ohlcv_start or date_str > ohlcv_end:
            continue

        actual_date = find_split_date(sym, date_str, factor)
        if not actual_date:
            print(f"  Dropping {sym}: no price match for {date_str} factor={factor:.2f}")
            sym_dropped = True
            break

        if actual_date:
            sym_splits[actual_date] = factor

    if sym_dropped:
        dropped_symbols.append(sym)
    elif sym_splits:
        splits[sym] = sym_splits

print(f"Processed {sum(len(v) for v in splits.values())} splits across {len(splits)} symbols")
if dropped_symbols:
    print(f"Dropped {len(dropped_symbols)} symbols due to unverifiable effective_date splits")

s3.put_object(Bucket=bucket, Key="config/splits.json", Body=json.dumps(splits))

# I am also manually adjusting the dividends here because IDK what goes into
# LSEG's adjusted dividend data. They might include other factors that I'd
# rather not include.
dividends = json.loads(s3.get_object(Bucket=bucket, Key="config/dividends.json")["Body"].read())
adjusted_dividends = {}
for pay_date, syms in dividends.items():
    pay_date_divs = {}
    for sym, info in syms.items():
        ex_date, amount = pd.Timestamp(info['ex_date']), info['amount']
        for split_date_str, factor in splits.get(sym, {}).items():
            if ex_date < pd.Timestamp(split_date_str):
                amount *= factor
        pay_date_divs[sym] = {'amount': amount, 'ex_date': info['ex_date']}
    if pay_date_divs:
        adjusted_dividends[pay_date] = pay_date_divs

s3.put_object(Bucket=bucket, Key="config/dividends_adjusted.json", Body=json.dumps(adjusted_dividends))
print("Wrote adjusted dividends to config/dividends_adjusted.json")

final_ohlcv_complete = [s for s in ohlcv_complete_symbols if s not in dropped_symbols]
s3.put_object(Bucket=bucket, Key="config/ohlcv_complete_symbols.json", Body=json.dumps(final_ohlcv_complete))
print(f"Wrote {len(final_ohlcv_complete)} OHLCV-complete symbols to config/ohlcv_complete_symbols.json")
