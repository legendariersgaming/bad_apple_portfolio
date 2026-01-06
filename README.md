# Bad Apple but it managed my stock portfolio for 2025

OK, it didn't actually manage my stock portfolio in 2025. This project is more
of an academic and artistic project and should not be taken as a trading
strategy. In fact, this strategy is impossible to replicate in real life since
one of the steps involves using knowledge of the next year's worth of
trajectories of every stock trading on Nasdaq. Without this step, the trading
strategy will lose money with probability close to 1. In fact, most assignments
lose almost the entire account monotonically. So, if you do implement this in
real life, then it's your own damn fault if you lose money. I warned you. Just
to emphasize the point:

## Disclaimer

**This is an academic and artistic project, not a trading strategy.**

- The transaction costs required to execute this strategy would be catastrophic.
- The "storyline" and "profit" shown in the video are manufactured by optimizing
  asset selection with future knowledge.
- **Do not attempt to trade this.**

## Results

You can see the final video [here](https://youtu.be/nu20pOxeIXc).

## Documentation

I've divided the documentation into three pieces.

- **[TECHNICAL.md](TECHNICAL.md)** contains the mathematical detail for how I
  modeled the portfolio dynamics, optimized the ticker-to-pixel assignment, and
  processed the data.
- **[COMMENTARY.md](COMMENTARY.md)** contains personal notes from building this
  project.
- **[REPRODUCTION.md](REPRODUCTION.md)** contains step-by-step instructions for
  running the data pipeline and reproducing the simulation. For the time being,
  the animation code is private. It is horrifically messy and I would be
  embarrassed to publish it without cleaning it up first. Not that this codebase
  is anything special either, but given that the majority of the work in this
  codebase is processing financial data, I consider it excusable. Working with
  financial data is an advanced form of torture and no amount of prettification
  will make it any better.

## The pipeline

If you don't want to read the documentation, then here's a quick summary. The
portfolio consists of 3,072 active positions chosen from a universe of 5,180 US
equities. The portfolio is rebalanced every 15 minutes of the trading year
(2024-12-10 to 2026-01-01), except for the open and close of the trading day.

The code in this repository reproduces the *results* (the optimized portfolio
and backtest). It consists of a data pipeline that ingests high-resolution
market data (BBO-1s) and corporate actions, processes the data into 15-minute
snapshots using cloud compute, solves a linear assignment problem to map stock
tickers to video pixels, and backtests the result to generate the performance
metrics seen in the video. Assets are filtered to include only those with
complete price data (both intraday BBO and daily OHLCV) and verifiable corporate
action dates.

## Cost breakdown

I spent approximately **$350** to produce this project. Well, ideally I would
have. In reality it was probably closer to $500 for some reasons I'll mention
in a moment.

Almost the entire cost was due to data. I purchased one month of the Databento
US Equities subscription ($200), which included a full year of BBO-1s data and
as much OHLCV-1d data as I needed. To backfill the remaining month, I spent
another ~$125 in on-demand usage. If you are smarter than me, then you can use
BBO-1m instead of BBO-1s to reduce the cost significantly. At first I had
greater ambitions, until I realized that a bit over 1 year of BBO-1s data for
3,072 assets is not a small amount of data to import and iterate through for a
backtest, and for what? Even rendering the video at 120 fps I would need, at
worst, 3-minute snapshots of the data.

The remaining cost was due to AWS S3 storage. The storage itself was *de
minimis* but uploading and downloading the data was a bit expensive. If you run
the pipeline once, then it might not be so onerous. In my case, I got close to
$100 from repeated uploading and downloading after running the pipeline at
least a dozen times.

Cloud compute through Modal should have been free for me since I used the free
starter tier, and the pipeline doesn't use all of the complimentary $30. But,
as stated before, I ran the pipeline multiple times during experimentation, so
I actually spent something like $50 on compute (on top of the complimentary
$30).

Corporate actions data was obtained via an institutional academic license for
LSEG.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file
for details.
