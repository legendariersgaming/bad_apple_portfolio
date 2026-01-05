import argparse
import os

import databento as db

import config  # noqa: F401

parser = argparse.ArgumentParser()
parser.add_argument("--start-date", required=True)
parser.add_argument("--end-date", required=True)
parser.add_argument("--dataset", default="XNAS.ITCH")
parser.add_argument("--symbols", default="ALL_SYMBOLS")
args = parser.parse_args()

client = db.Historical(os.environ["DATABENTO_API_KEY"])

print(f"Submitting OHLCV-1d batch job for {args.symbols}...")
job = client.batch.submit_job(
    dataset=args.dataset,
    symbols=args.symbols.split(",") if "," in args.symbols else args.symbols,
    schema="ohlcv-1d",
    start=args.start_date,
    end=args.end_date,
    encoding="dbn",
    compression="zstd",
)

print(f"Job ID: {job['id']}")
print(f"Status: {job['state']}")
print(f"Cost: ${job.get('cost_usd', 'pending')}")
