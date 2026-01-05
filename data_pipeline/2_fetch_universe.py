import databento as db
import pandas as pd

from config import DATA_DIR as OUTPUT_DIR

RIC_SUFFIX = {
    "XNAS": ".OQ", "XNYS": ".N", "ARCX": ".P", "BATS": ".Z",
    "BATY": ".BT", "EDGA": ".EA", "EDGX": ".EX", "XASE": ".A", "IEXG": ".IE",
}
EXCLUDE_SUFFIXES = ["W", "R", "U", "+", "=", "^"]

env_file = Path(".env")
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

frames = pd.read_parquet(OUTPUT_DIR / "bad_apple_frames.parquet", columns=["timestamp"])
start_date = frames["timestamp"].min().strftime("%Y-%m-%d")
end_date = (frames["timestamp"].max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
print(f"Date range from frames: {start_date} to {end_date}")

client = db.Historical()
defs = client.timeseries.get_range(
    dataset="XNAS.ITCH", symbols="ALL_SYMBOLS", schema="definition",
    start=start_date, end=end_date
)
instruments = defs.to_df()
symbol_col = "raw_symbol" if "raw_symbol" in instruments.columns else "symbol"
instruments = instruments.drop_duplicates(subset=[symbol_col], keep="last")
instruments["dataset"] = "XNAS.ITCH"

df = instruments[instruments["instrument_class"] == "K"].copy()
for suffix in EXCLUDE_SUFFIXES:
    df = df[~df[symbol_col].str.endswith(suffix)]

df["RIC"] = df[symbol_col] + df["exchange"].map(RIC_SUFFIX)
df = df.dropna(subset=["RIC"])
universe = df[["RIC", symbol_col, "exchange", "instrument_class", "currency"]].copy()
universe.columns = ["RIC", "symbol", "exchange", "instrument_class", "currency"]
universe = universe.drop_duplicates(subset=["RIC"])

universe[["RIC"]].to_csv(OUTPUT_DIR / "databento_universe_rics.csv", index=False)
print(f"Saved {len(universe)} symbols to universe files")
