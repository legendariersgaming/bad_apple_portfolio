import json
import os
import tempfile
import time
from collections import defaultdict

import lseg.data as ld
import pandas as pd
from tqdm import tqdm

from config import DATA_DIR, get_s3_client, get_s3_bucket

import warnings  # LSEG fix your warnings please and thank you
warnings.filterwarnings("ignore", category=FutureWarning)

INPUT_FILE = DATA_DIR / "databento_universe_rics.csv"
FRAMES_FILE = DATA_DIR / "bad_apple_frames.parquet"

# Dammit LSEG why do you filter corporate actions data based on the
# ANNOUNCEMENT date??
START_DATE = "2020-01-01"
END_DATE = "2026-01-01"
BATCH_SIZE = 100
MAX_RETRIES = 3


def fetch_with_retry(func, desc="request"):
    for attempt in range(MAX_RETRIES):
        try:
            df = func()
            return df if df is not None and not df.empty else None
        except Exception as e:
            wait = 2 ** (attempt + 1)
            if attempt < MAX_RETRIES - 1:
                tqdm.write(f"Retry {attempt + 1}/{MAX_RETRIES} for {desc} after {wait}s: {e}")
                time.sleep(wait)
            else:
                tqdm.write(f"FAILED {desc} after {MAX_RETRIES} retries: {e}")
    return None


rics = pd.read_csv(INPUT_FILE)["RIC"].tolist()

all_dividends = []
all_capital_changes = []
lseg_covered_rics = set()

with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    json.dump({
        "sessions": {"platform": {"rdp": {
            "app-key": os.getenv("LSEG_APP_KEY") or os.getenv("REFINITIV_API_KEY"),
            "username": os.getenv("LSEG_USERNAME") or os.getenv("REFINITIV_USERNAME"),
            "password": os.getenv("LSEG_PASSWORD") or os.getenv("REFINITIV_PASSWORD"),
            "signon_control": True,
        }}},
        "logs": {"level": "warning"},
    }, f)
    config_path = f.name
ld.load_config(config_path)
os.unlink(config_path)
ld.open_session(name="platform.rdp")

try:
    print("Checking LSEG coverage...")
    for i in tqdm(range(0, len(rics), BATCH_SIZE), desc="Coverage"):
        batch = rics[i:i+BATCH_SIZE]
        df = fetch_with_retry(lambda: ld.get_data(batch, ["TR.CommonName"]), f"coverage batch {i//BATCH_SIZE + 1}")
        if df is not None:
            covered = df[df["Company Common Name"].notna()]["Instrument"].tolist()
            lseg_covered_rics.update(covered)

    print(f"LSEG covers {len(lseg_covered_rics)} of {len(rics)} RICs")
    covered_rics = sorted(lseg_covered_rics)

    print("Fetching corporate actions for covered symbols...")
    for i in tqdm(range(0, len(covered_rics), BATCH_SIZE), desc="Corp Actions"):
        batch = covered_rics[i:i+BATCH_SIZE]

        df = fetch_with_retry(lambda: ld.get_data(batch, [
            "TR.DivExDate", "TR.DivPayDate", "TR.DivRecordDate",
            "TR.DivUnadjustedGross", "TR.DivAdjustedGross", "TR.DivType", "TR.DivCurrency"
        ], parameters={"SDate": START_DATE, "EDate": END_DATE}), f"dividends batch {i//BATCH_SIZE + 1}")
        if df is not None:
            df = df.rename(columns={
                "Instrument": "ric", "Dividend Ex Date": "ex_date", "Dividend Pay Date": "pay_date",
                "Dividend Record Date": "record_date", "Gross Dividend Amount": "gross_amount",
                "Adjusted Gross Dividend Amount": "adjusted_amount", "Dividend Type": "div_type"})
            df = df.dropna(subset=["ex_date"])
            if not df.empty:
                all_dividends.append(df)

        df = fetch_with_retry(lambda: ld.get_data(batch, [
            "TR.CAExDate", "TR.CAEffectiveDate", "TR.CAAdjustmentFactor", "TR.CAAdjustmentType",
            "TR.CAAnnouncementDate", "TR.CATermsOldShares", "TR.CATermsNewShares"
        ], parameters={"SDate": START_DATE, "EDate": END_DATE}), f"capital changes batch {i//BATCH_SIZE + 1}")
        if df is not None:
            df = df.rename(columns={
                "Instrument": "ric", "Capital Change Ex Date": "ex_date",
                "Capital Change Effective Date": "effective_date",
                "Adjustment Factor": "adjustment_factor", "Adjustment Type": "adjustment_type",
                "Capital Change Announcement Date": "announcement_date",
                "Terms Old Shares": "terms_old_shares", "Terms New Shares": "terms_new_shares"})

            # LSEG returns a row for every queried symbol, even those with no
            # capital changes. Those placeholder rows have all NaN values, so
            # we drop rows where BOTH ex_date and effective_date are missing.
            df = df.dropna(subset=["ex_date", "effective_date"], how="all")
            if not df.empty:
                all_capital_changes.append(df)
finally:
    ld.close_session()

dividends_df = pd.concat(all_dividends, ignore_index=True) if all_dividends else pd.DataFrame()
capital_changes_df = pd.concat(all_capital_changes, ignore_index=True) if all_capital_changes else pd.DataFrame()

print(f"Fetched {len(dividends_df)} dividends, {len(capital_changes_df)} capital changes")

splits_dict = {}
for _, row in capital_changes_df.iterrows():
    sym = row['ric'].rsplit('.', 1)[0]
    adj = row['adjustment_factor']
    if pd.isna(adj) or adj == 0 or adj == 1:
        continue

    if pd.notna(row['ex_date']):
        date_str = str(row['ex_date'].date())
        date_type = "ex_date"
    elif pd.notna(row['effective_date']):
        date_str = str(row['effective_date'].date())
        date_type = "effective_date"
    else:
        continue

    if sym not in splits_dict:
        splits_dict[sym] = []
    splits_dict[sym].append({
        "date": date_str,
        "factor": adj,
        "date_type": date_type
    })

divs_by_date = defaultdict(dict)
for _, row in dividends_df.iterrows():
    if pd.isna(row['gross_amount']) or row['gross_amount'] <= 0 or pd.isna(row['pay_date']):
        continue
    sym = row['ric'].rsplit('.', 1)[0]
    divs_by_date[str(row['pay_date'].date())][sym] = {
        'amount': row['gross_amount'],
        'ex_date': str(row['ex_date'].date())
    }

lseg_symbols = sorted(set(ric.rsplit('.', 1)[0] for ric in lseg_covered_rics))
pd.DataFrame({"symbol": lseg_symbols}).to_csv(DATA_DIR / "lseg_covered_symbols.csv", index=False)
print(f"Saved {len(lseg_symbols)} LSEG-covered symbols to lseg_covered_symbols.csv")

s3 = get_s3_client()
bucket = get_s3_bucket()

s3.put_object(Bucket=bucket, Key="config/splits_lseg.json", Body=json.dumps(splits_dict))
s3.put_object(Bucket=bucket, Key="config/dividends.json", Body=json.dumps(dict(divs_by_date)))

total_splits = sum(len(v) for v in splits_dict.values())
ex_date_count = sum(1 for v in splits_dict.values() for s in v if s["date_type"] == "ex_date")
eff_date_count = total_splits - ex_date_count
print(f"Uploaded splits_lseg.json ({len(splits_dict)} symbols, {total_splits} splits: {ex_date_count} ex_date, {eff_date_count} effective_date)")
print(f"Uploaded dividends.json ({len(divs_by_date)} pay dates)")
