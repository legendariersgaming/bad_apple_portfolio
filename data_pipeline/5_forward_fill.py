import os

import modal

image = modal.Image.debian_slim().pip_install("databento", "pandas", "pyarrow", "boto3", "exchange_calendars")
app = modal.App("bad-apple-forward-fill", image=image)

INTERVAL_15MIN_NS = 15 * 60 * 1_000_000_000
INTERVAL_1MIN_NS = 60 * 1_000_000_000


def _get_s3_client_remote():
    import boto3
    return boto3.client("s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])


def build_symbology_for_date(all_symbology_files, date_str):
    mapping = {}
    for sym_data in all_symbology_files:
        file_start = sym_data["start_date"][:10]
        file_end = sym_data["end_date"][:10]
        if not (file_start <= date_str < file_end):
            continue
        for symbol, entries in sym_data.get("result", {}).items():
            for entry in entries:
                d0, d1 = entry["d0"], entry["d1"]
                if d0 <= date_str < d1:
                    inst_id = str(entry["s"])
                    if inst_id in mapping and mapping[inst_id] != symbol:
                        raise ValueError(
                            f"Symbology conflict: instrument {inst_id} on {date_str} "
                            f"maps to both {mapping[inst_id]} and {symbol}"
                        )
                    mapping[inst_id] = symbol
    return mapping


@app.function(secrets=[modal.Secret.from_name("bad-apple")], timeout=3600, memory=65536)
def process_day(date_str, all_symbology_files):
    import io
    import databento as db
    import pandas as pd
    import exchange_calendars as xcals
    from botocore.exceptions import ClientError

    s3 = _get_s3_client_remote()
    bucket = os.environ["S3_BUCKET_NAME"]

    def exists(key):
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    need_15min = not exists(f"bbo_15min/{date_str}.parquet")
    need_1min = not exists(f"bbo_1min/{date_str}.parquet")

    if not need_15min and not need_1min:
        return f"SKIP {date_str}: already processed"

    schedule = xcals.get_calendar("XNAS").schedule.loc[date_str:date_str]
    if schedule.empty:
        return f"SKIP {date_str}: market closed"

    symbology = build_symbology_for_date(all_symbology_files, date_str)
    if not symbology:
        return f"SKIP {date_str}: no symbology"

    market_open_ns = int(schedule.iloc[0]["open"].value)
    market_close_ns = int(schedule.iloc[0]["close"].value)
    date_compact = date_str.replace("-", "")

    bbo_local = f"/tmp/bbo_{date_str}.dbn.zst"
    s3.download_file(bucket, f"bbo/xnas-itch-{date_compact}.bbo-1s.dbn.zst", bbo_local)
    bbo_df = db.DBNStore.from_file(bbo_local).to_df().reset_index()
    bbo_df['ts_recv'] = bbo_df['ts_recv'].astype('int64')
    bbo_df = bbo_df[(bbo_df['ts_recv'] < market_close_ns) & (bbo_df['bid_px_00'] > 0) & (bbo_df['ask_px_00'] > 0)].copy()
    bbo_df['symbol'] = bbo_df['instrument_id'].astype(str).map(symbology)
    bbo_df = bbo_df.dropna(subset=['symbol'])
    bbo_df['mid'] = (bbo_df['bid_px_00'] + bbo_df['ask_px_00']) / 2
    bbo_df['spread_bps'] = (bbo_df['ask_px_00'] - bbo_df['bid_px_00']) / bbo_df['mid'] * 10000
    bbo_df['ts'] = pd.to_datetime(bbo_df['ts_recv'], unit='ns', utc=True)
    bbo_df = bbo_df.sort_values('ts')
    os.remove(bbo_local)

    results = []

    def resample_bbo(interval_ns, output_key):
        period_starts = pd.to_datetime(list(range(market_open_ns, market_close_ns, interval_ns)), unit='ns', utc=True)
        period_df = pd.DataFrame({'period': period_starts})
        bbo_results = []
        for sym, grp in bbo_df.groupby('symbol'):
            merged = pd.merge_asof(period_df, grp[['ts', 'mid', 'spread_bps']], left_on='period', right_on='ts', direction='backward').dropna()
            merged['symbol'] = sym
            bbo_results.append(merged[['symbol', 'period', 'mid', 'spread_bps']])
        final = pd.concat(bbo_results, ignore_index=True).sort_values(['symbol', 'period'])
        buf = io.BytesIO()
        final.to_parquet(buf, index=False)
        buf.seek(0)
        s3.put_object(Bucket=bucket, Key=output_key, Body=buf.getvalue())
        return len(final)

    if need_15min:
        n = resample_bbo(INTERVAL_15MIN_NS, f"bbo_15min/{date_str}.parquet")
        results.append(f"15min={n}")

    if need_1min:
        n = resample_bbo(INTERVAL_1MIN_NS, f"bbo_1min/{date_str}.parquet")
        results.append(f"1min={n}")

    return f"SUCCESS {date_str}: {', '.join(results)}"


@app.local_entrypoint()
def main():
    import json
    from pathlib import Path
    from config import get_s3_client, get_s3_bucket

    s3 = get_s3_client()
    bucket = get_s3_bucket()

    dates = set()
    for obj in s3.list_objects_v2(Bucket=bucket, Prefix="bbo/").get("Contents", []):
        filename = obj["Key"].split("/")[-1]
        if filename.endswith(".dbn.zst") and "symbology" not in filename:
            date_part = filename.split("-")[2].split(".")[0]
            dates.add(f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}")

    dates = sorted(dates)
    print(f"Processing {len(dates)} days: {dates[0]} to {dates[-1]}")

    all_symbology_files = []
    for obj in s3.list_objects_v2(Bucket=bucket, Prefix="bbo/symbology_").get("Contents", []):
        sym_data = json.loads(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
        all_symbology_files.append(sym_data)
        print(f"Loaded {obj['Key']}: {sym_data['start_date'][:10]} to {sym_data['end_date'][:10]}")
    print(f"Total symbology files: {len(all_symbology_files)}")

    for res in process_day.map(dates, kwargs={"all_symbology_files": all_symbology_files}):
        print(res)

    print("Downloading bbo_15min data...")
    bbo_dir = Path("data/bbo_15min")
    bbo_dir.mkdir(parents=True, exist_ok=True)
    for obj in s3.list_objects_v2(Bucket=bucket, Prefix="bbo_15min/").get("Contents", []):
        filename = obj["Key"].split("/")[-1]
        if filename.endswith(".parquet"):
            s3.download_file(bucket, obj["Key"], str(bbo_dir / filename))
    print(f"Downloaded to {bbo_dir}")
