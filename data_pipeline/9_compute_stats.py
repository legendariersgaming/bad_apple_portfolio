import json
import pandas as pd
import numpy as np
import yfinance as yf
import exchange_calendars as xcals
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

nav = pd.read_parquet(DATA_DIR / "backtest_nav.parquet")
nav = nav.set_index("period").sort_index()

cal = xcals.get_calendar("XNAS")
schedule = cal.schedule.loc["2024-12-10":"2025-12-31"]
trading_minutes_per_year = (schedule["close"] - schedule["open"]).dt.total_seconds().sum() / 60

initial_nav = nav["nav"].iloc[0]
final_nav = nav["nav"].iloc[-1]
total_return = final_nav / initial_nav - 1

returns = nav["nav"].pct_change().dropna()
n_minutes = len(returns)
annualization_factor = trading_minutes_per_year / n_minutes
annualized_return = (1 + total_return) ** annualization_factor - 1
annualized_vol = returns.std() * np.sqrt(trading_minutes_per_year)

tbill = yf.download("^IRX", start=nav.index[0], end=nav.index[-1], progress=False)
risk_free_rate = tbill["Close"].mean().item() / 100
sharpe_ratio = (annualized_return - risk_free_rate) / annualized_vol

cummax = nav["nav"].cummax()
drawdown = (nav["nav"] - cummax) / cummax
max_drawdown = drawdown.min()

stats = {
    "period_start": str(nav.index[0].date()),
    "period_end": str(nav.index[-1].date()),
    "initial_nav": float(initial_nav),
    "final_nav": float(final_nav),
    "total_return": float(total_return),
    "annualized_return": float(annualized_return),
    "annualized_volatility": float(annualized_vol),
    "risk_free_rate": float(risk_free_rate),
    "sharpe_ratio": float(sharpe_ratio),
    "max_drawdown": float(max_drawdown),
}

with open(DATA_DIR / "backtest_stats.json", "w") as f:
    json.dump(stats, f, indent=2)

print(f"Period: {nav.index[0].date()} to {nav.index[-1].date()}")
print(f"Initial NAV: ${initial_nav:,.2f}")
print(f"Final NAV:   ${final_nav:,.2f}")
print(f"Total return: {total_return * 100:.2f}%")
print(f"Annualized return: {annualized_return * 100:.2f}%")
print(f"Annualized volatility: {annualized_vol * 100:.2f}%")
print(f"Sharpe ratio: {sharpe_ratio:.2f} (rf={risk_free_rate*100:.2f}%)")
print(f"Max drawdown: {max_drawdown * 100:.2f}%")
print(f"\nStats saved to {DATA_DIR / 'backtest_stats.json'}")

