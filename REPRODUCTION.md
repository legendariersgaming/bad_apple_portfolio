# Reproduction guide

This guide outlines the steps required to reproduce the Bad Apple portfolio
simulation from scratch.

**Cost estimates**
*   **Data (Databento).** I spent ~$325 for BBO-1s data. Using BBO-1m instead
    could significantly reduce this.
*   **LSEG.** Costs vary by institution. Commercial access to this data is
    usually expensive.
*   **Compute (Modal).** The data processing can run within the $30/month
    starter tier.
*   **Storage (S3).** The full pipeline generates ~350 GB of data.

## Prerequisites

### 1. Software
- **Python 3.10+**
- **uv** (recommended for dependency management) or pip.
- **Modal** CLI (installed via project dependencies)
- **AWS CLI** (configured with access to S3)

### 2. Services & accounts
- **AWS S3.** You need a bucket to store the raw and processed data (~100s of
  GBs).
- **Databento.** Account with API access to `XNAS.ITCH` (Nasdaq TotalView) data.
- **LSEG / Refinitiv.** Account with API access for corporate actions
  (`lseg.data` library).
- **Modal.** Account for running the parallel processing jobs
  (`5_forward_fill.py`).

## Installation

Install the project dependencies (including `modal`):

```bash
uv sync
```

## Configuration

Create a `.env` file in the root directory (or ensure these variables are in
your environment) that contains the following (with the `...` replaced with your
actual values).

```bash
DATABENTO_API_KEY=db_...
LSEG_APP_KEY=...
LSEG_USERNAME=...
LSEG_PASSWORD=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET_NAME=...
```

### Modal setup
Authenticate with Modal and create the secrets required for cloud workers to
access your S3 bucket.

```bash
uv run modal setup
uv run modal secret create bad-apple AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... S3_BUCKET_NAME=...
```

## Running the pipeline

Execute the scripts in `data_pipeline/` sequentially.

### 1. Data ingestion
1.  **Download Video.** `uv run python data_pipeline/1_download_video.py`
    - Downloads the Bad Apple video and processes it into a parquet file of
      frames.
2.  **Fetch Universe.** `uv run python data_pipeline/2_fetch_universe.py`
    - Queries Databento to find all `K` (stock) symbols active during the
      period.
3.  **Corporate Actions.** `uv run python data_pipeline/3_corporate_actions.py`
    - *Requires LSEG access.* Downloads corporate action adjustment factors and
      dividend history for the universe.
    - **Important:** The script fetches from 2020-01-01 (not the simulation
      start date) because LSEG filters by announcement date, not effective
      date. Actions announced before the query start date are excluded even if
      their effective date falls within the simulation period.
    - For each adjustment, LSEG provides both an ex-date (`TR.CAExDate`) and an
      effective date (`TR.CAEffectiveDate`). Neither field seems to be
      reliable, hence all dates are verified against price data in step 6.
    - Saves to `config/splits_lseg.json` and `config/dividends.json` in S3.

### 3. Market data processing
4.  **Batch Request (DataBento).** `uv run python data_pipeline/4a_batch_bbo.py
    --start-date 2024-12-10 --end-date 2026-01-01`
    - **COST WARNING:** This submits a large batch job to Databento.
    - Also run `uv run python data_pipeline/4b_batch_ohlcv.py` with the same
      date range for daily data used in split verification.
    - After the batch downloads are ready, run `uv run python
      data_pipeline/4c_ingest_to_s3.py --job-id ... --prefix bbo` with the BBO
      job ID and `uv run python data_pipeline/4c_ingest_to_s3.py --job-id ...
      --prefix ohlcv` with the OHLCV job ID to move the resulting data from
      Databento's servers to your S3 bucket. (You can use the `--workers` flag
      to control the number of workers. I had to use 4 workers because my disk
      space is severely limited.)
5.  **Forward Fill & Resampling.** `uv run modal run
    data_pipeline/5_forward_fill.py`
    - **Runs on Modal.**
    - Spins up cloud workers to process the raw BBO data into 15-minute and
      1-minute snapshots.
    - Uploads the processed snapshots to S3 (`bbo_15min/*.parquet` and
      `bbo_1min/*.parquet`).
    - Downloads the processed `bbo_15min` data to `data/bbo_15min/` locally.

### 4. Simulation
6.  **Apply Splits.** `uv run python data_pipeline/6_apply_splits.py`
    - Verifies and applies LSEG's corporate action adjustments.
    - All adjustments are verified against OHLCV data by searching for the
      actual price discontinuity. If no match is found, the symbol is dropped
      from the backtest entirely.
    - Identifies symbols with complete OHLCV-1d data (required for reliable
      adjustment date verification).
    - Adjusts dividend amounts for any adjustments occurring after the split.
    - Saves the following configuration files that are used by the optimizer
      and backtester.
        - `config/splits.json` contains verified mapping of symbol to adjustment
          events (date and factor).
        - `config/dividends_adjusted.json` contains dividend payments adjusted
          for corporate actions.
        - `config/ohlcv_complete_symbols.json` contains symbols with complete
          daily data, minus any dropped due to unverifiable adjustment dates.
7.  **Optimize Assignment.** `uv run python
    data_pipeline/7_optimize_assignment.py`
    - Solves the linear assignment problem.
    - Uses `data/bad_apple_narrative.parquet` to target the specific
      profit-and-loss storyline.
8.  **Backtest.** `uv run modal run data_pipeline/8_backtest.py`
    - Simulates the portfolio rebalancing using the optimized assignments.
    - Saves the following results to `data/`.
        - `backtest_nav.parquet` contains portfolio Net Asset Value (NAV),
          cash, and liquid NAV history at every minute.
        - `backtest_rebalances.parquet` contains detailed pre- and post-trade
          snapshots at each rebalance (pre/post NAV, liquid NAV, cash,
          positions, spread cost, and active pixel count).
        - `backtest_shares.parquet` contains share counts for every asset at
          every minute.
        - `backtest_values.parquet` contains position values ($) for every
          asset at every minute.
9.  **Compute Stats.** `uv run python data_pipeline/9_compute_stats.py`
    - Generates summary statistics.

## Output
The final artifacts (assignments, portfolio value history, etc.) will be in the
`data/` directory.
