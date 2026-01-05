# Bad Apple portfolio: technical description

**Disclaimer.** This document describes an artistic/technical demonstration and
does not constitute financial advice. The strategy described herein is not
intended for replication.

## 1. Project overview

First let me give a brief summary of the project and some technical caveats.
The rest of the document goes into full technical detail. For personal
commentary, see [COMMENTARY.md](COMMENTARY.md).

**Project motivation.** It's common in finance to represent portfolios as
vectors of *portfolio weights*, where the $`i`$th entry in the vector is the
fraction of your total portfolio value invested in the $`i`$th name. If you
reshape the vector of portfolio weights to form a 2D grid, then you get a
picture, and you can watch the picture change over time as you move capital
between names and the investments accrue or decay with market movements.

In September 2025, I had the idea to investigate what would happen if you
periodically rebalanced your portfolio over the course of a year so that, when
played back, the moving picture of portfolio weights would play Bad Apple.
Coincidentally, if you divide the trading year into 15-minute intervals, then
you obtain a count nearly identical to the number of frames in the standard Bad
Apple video played at 30 fps!

That's what this project is. I first download the Bad Apple video at 30 fps
with a 4:3 aspect ratio, and downsample so that each frame has 64 columns and
48 rows of pixels. I then pick out 3072 stocks and, at the start of the year,
assign the names uniquely to the pixel grid. I let each video frame correspond
to a 15-minute trading interval, and I rebalance at the start of each 15-minute
interval so that the resulting exposure weights are proportional to the
grayscale intensities of the corresponding pixels. (The exposure weight of a
name is the fraction of the *invested* capital held in that name. This differs
from the portfolio weight, which is the fraction of *total* capital including
cash.)

However, most assignments of tickers to pixels produce boring, monotonically
decreasing profit-and-loss charts. That is to say, Bad Apple is a Bad Strategy
in practice. (Sorry!) To make things interesting, I find an assignment that
produces a profit-and-loss chart that approximately follows a storyline of my
choosing. I decided to line up periods of gain and loss with general market
sentiment at various points throughout the year. That is what you see in the
final video.

**Some technical caveats.** While I have violated causality by first filtering
the universe of stocks to select only those for which I have sufficient data to
cover the whole year, then picking a ticker-to-pixel assignment that produces
interesting backtest results, I have otherwise tried to stay faithful to market
conditions within my time and expense budget. Below is a surely nonexhaustive
list of simplifying assumptions made out of convenience or necessity.
- I assume all trades happen at the best bid or best ask on offer at the time
  of rebalancing. In particular, I assume that my trades do not eat through the
  order book.
- I assume that my trades have no lingering market impact.
- I permit the holding of arbitrary fractional shares at no additional cost,
  which simplifies split adjustment and rebalance calculations.
- I ignore the cost of margin. I also ignore margin rules in general, though I
  don't think I ever violate them.
- I assume that all adjustment factors that I pulled from LSEG apply to both
  the price and dividends.
- Spin-offs are present in the data set. I apply the corresponding adjustment
  factor without special handling (*e.g.*, I do not identify the spin-off
  ticker and account for the splitting of ownership), which likely introduces
  minor inaccuracies.

---

Now we can get into the technical detail of the assignment problem and
backtest. I'll discuss the details abstractly first and then explain my
specific instantiation.

## 2. Time and asset conventions

**Time convention.** Throughout this document, we index time by $`t \in
\mathbf{Z}_+`$, measured in minutes from some epoch (say, the Unix epoch). Let
$`\mathcal{T} = \{\tau_1, \tau_2, \ldots, \tau_T\} \subset \mathbf{Z}_+`$
denote the ordered set of rebalance times (the opens of each 15-minute
interval), where $`T = \#\mathcal{T}`$ is the total number of intervals. Note
that if $`\tau_k`$ is the $`k`$th rebalance time, then $`\tau_{k-1}`$ is the
$`(k-1)`$st rebalance time while $`\tau_k - 1`$ refers to the literal clock
time occurring one minute before rebalance $`k`$.

To help distinguish pre- and post-trade states, for each rebalance time
$`\tau\in\mathcal{T}`$, we write $`\tau^{-}`$ for the instant immediately
before the rebalance, and $`\tau`$ for the state immediately after.

**Asset universe.** We consider a universe of $`n`$ equities traded on US
exchanges. Let $`[n] = \{1, \ldots, n\}`$ denote the set of asset labels.

**Target intensities.** Each rebalance time $`\tau \in \mathcal{T}`$
corresponds to a frame of the Bad Apple video. We assign each asset $`j \in
[n]`$ a *target intensity* $`f_{\tau,j} \in [0,1]`$ derived from the video
frame. Assets not selected for the portfolio receive intensity $`f_{\tau,j} =
0`$. The assignment process is discussed in section 4.

We say asset $`j`$ is *active* at time $`\tau`$ if $`f_{\tau,j} > 0`$. Let
$`A_\tau = \#\{j : f_{\tau,j} > 0\}`$ denote the count of active assets.

## 3. Portfolio state, valuation, and rebalancing

**State variables.** The portfolio state at time $`t`$ consists of
- the cash balance $`c_t \in \mathbf{R}`$ (USD);
- the holdings vector $`q_t \in \mathbf{R}_+^n`$ (split-adjusted shares); and
- the set $`\mathcal{D}_t`$ of pending dividends, whose elements are tuples
  $`(j, \tilde{q}_j, \delta_j, d_{\text{pay}})`$ representing dividend
  entitlements not yet paid, where $`j\in[n]`$ is the asset index,
  $`\tilde{q}_j`$ is the entitled share count, $`\delta_j`$ is the per-share
  dividend (adjusted for corporate actions), and $`d_{\text{pay}}`$ is the
  payment date.

We allow fractional shares. This ensures that rebalancing to target exposure
weights is exact (no rounding), and that corporate action adjustments are exact
(no rounding with cash compensation).

**Valuation prices.** At time $`t`$, let $`a_t, b_t \in \mathbf{R}_{++}^n`$
denote the vectors of best ask and bid prices, respectively. Let $`p_t \in
\mathbf{R}_{++}^n`$ denote the vector of midprices, $`p_t = (a_t + b_t)/2`$.
All prices are split-adjusted.

**Portfolio valuation.** The position value is $`P_t = q_t^\top p_t`$. Define
the pending dividend value as

```math
D_t = \sum_{(j, \tilde{q}_j, \delta_j, d) \in \mathcal{D}_t} \tilde{q}_j \delta_j,
```

the total cash owed but not yet received. We distinguish two notions of net
asset value: *liquid NAV* and *total NAV*. Liquid NAV $`L_t`$ is the investable
capital available for rebalancing, given by

```math
L_t = c_t + P_t.
```

Total NAV $`V_t`$ is the liquid NAV, plus any pending dividends, *i.e.*,

```math
V_t = c_t + P_t + D_t = L_t + D_t.
```

Rebalancing targets allocations relative to $`L_t`$, so pending dividends do
not affect position sizing.

**Rebalancing.** At each rebalance time $`\tau \in \mathcal{T}`$, we rebalance
to match the target frame. The procedure is as follows.

First, compute the pre-trade liquid NAV

```math
L_{\tau^-} = c_{\tau^-} + q_{\tau^-}^\top p_\tau.
```

Next, compute the *per-asset allocation*. If $`A_\tau > 0`$, then each active
asset receives an equal share of liquid NAV, *i.e.*, is allotted up to

```math
\alpha_\tau = \frac{L_{\tau^-}}{A_\tau}
```

in cash that can be invested. The target position value for asset $`j`$ is

```math
v_{\tau,j}^* = f_{\tau,j} \alpha_\tau,
```

and the target share count is

```math
q_{\tau,j}^* = \frac{v_{\tau,j}^*}{p_{\tau,j}} = \frac{f_{\tau,j} \alpha_\tau}{p_{\tau,j}}.
```

When $`A_\tau = 0`$ (a black frame), we set $`q_{\tau,j}^* = 0`$ for all $`j`$
and the portfolio holds only cash.

Define the trade quantity $`\Delta_j = q_{\tau,j}^* - q_{\tau^-,j}`$. We
execute at the price $`x_{\tau,j}\in\mathbf{R}_{++}`$ given by

```math
x_{\tau,j} = \begin{cases} a_{\tau,j} & \text{if } \Delta_j > 0 \text{ (buy)}, \\ b_{\tau,j} & \text{if } \Delta_j < 0 \text{ (sell)}, \\ p_{\tau,j} & \text{if } \Delta_j = 0 \text{ (hold)}. \end{cases}
```

The post-rebalance holdings are $`q_\tau = q_\tau^*`$, and the post-rebalance
cash is

```math
c_\tau = c_{\tau^-} - \Delta^\top x_{\tau}.
```

Transaction costs are implicitly captured by executing at bid/ask rather than
mid. Valuation uses midprices, so spread costs immediately appear as some loss
to NAV.

**Intra-interval dynamics.** Between rebalances, cash, holdings, and pending
dividends remain constant. Let $`\tau, \tau' \in \mathcal{T}`$ be consecutive
rebalance times. For $`t \in (\tau, \tau')`$,

```math
c_t = c_\tau, \qquad q_t = q_\tau, \qquad \text{and}\qquad \mathcal{D}_t = \mathcal{D}_\tau.
```

The valuation prices $`p_t`$ evolve with market quotes, and consequently
$`P_t`$ and $`V_t`$ drift between rebalances.

**Allocations and frame visualization.** Between rebalances, allocations drift
with prices. Define the *drifting allocation* for asset $`j`$ at time $`t`$ as

```math
\alpha_{t,j} = \frac{p_{t,j}}{p_{\tau,j}} \alpha_\tau,
```

where $`\tau=\sup\{s\in\mathcal{T} : s\leq t\}`$ is the most recent rebalance
time. This simply scales the rebalance-time allocation by the asset's price
change.

With this definition, the corresponding frame intensity is

```math
\begin{aligned}
f_{t,j} = \frac{v_{t,j}}{\alpha_{t,j}} &= \frac{q_{\tau,j} p_{t,j}}{\alpha_\tau p_{t,j}/p_{\tau,j}}\\
&= \frac{q_{\tau,j} p_{\tau,j}}{\alpha_\tau}\\
&= f_{\tau,j},
\end{aligned}
```

which is the frame intensity at the most recent rebalance time. That is to say,
since position values and allocations scale together with market prices, their
ratio remains invariant between rebalances.

**Overnight adjustments.** Let $`t_d^o`$ denote the market open time of day
$`d`$ and let $`t_d^c`$ denote the market close time. Corporate actions are
applied overnight, between $`t_d^c`$ and $`t_{d+1}^o`$.

*Corporate action adjustments.* Since all prices are adjusted upfront (to the
end of the backtest period), the price series is continuous across adjustment
dates. As mentioned in the state description, shares are actually adjusted
shares, so the position value $`q_t^\top p_t`$ remains correct. No adjustment
of shares or execution prices is required.

*Dividends.* At market open on day $`d+1`$, we update the pending dividend set
in two steps. First, we credit dividends with payment date $`d+1`$ to cash and
remove them. This updates the cash and gives an intermediate set
$`\mathcal{D}'`$ of pending dividends remaining. More precisely,

```math
c_{t_{d+1}^o} = c_{t_d^c} + \sum_{(j, \tilde{q}_j, \delta_j, d+1) \in \mathcal{D}_{t_d^c}} \tilde{q}_j \delta_j, \qquad \mathcal{D}' = \mathcal{D}_{t_d^c} \setminus \{(j, \tilde{q}_j, \delta_j, d') \in \mathcal{D}_{t_d^c} : d' = d+1\}.
```

Second, we record new entitlements for assets going ex-dividend on day $`d+1`$.
Let $`E_{d+1} = \{(j, \delta_j, d_{\text{pay}}) : \text{asset } j \text{ goes
ex-div on } d+1\}`$. Then

```math
\mathcal{D}_{t_{d+1}^o} = \mathcal{D}' \cup \{(j, q_{t_d^c,j}, \delta_j, d_{\text{pay}}) : (j, \delta_j, d_{\text{pay}}) \in E_{d+1}\}.
```

## 4. Assignment optimization

**Qualitative objective.** The assignment is chosen to produce a narratively
interesting profit-and-loss trajectory over the backtest period.

**Pixel-to-asset mapping.** The Bad Apple video has resolution $`H \times W`$
pixels. Let $`m = HW`$ denote the pixel count per frame. We assume $`m \leq n`$
(more assets than pixels). Each frame at rebalance time $`\tau`$ is a matrix
$`F_\tau \in [0,1]^{H \times W}`$ of grayscale intensities, which we flatten to
a vector $`\hat{f}_\tau \in [0,1]^m`$ in row-major order.

We introduce $`n`$ *slots*, the first $`m`$ of which correspond to pixels, and
the remaining $`n - m`$ serving as dummy slots with intensity zero. Define the
zero-padded intensity vector $`\tilde{f}_\tau \in [0,1]^n`$ by

```math
\tilde{f}_{\tau,i} = \begin{cases} \hat{f}_{\tau,i} & \text{if } i \leq m, \\ 0 & \text{if } i > m. \end{cases}
```

The assignment is encoded by a permutation $`\pi : [n] \to [n]`$, where
$`\pi(j)`$ is the slot assigned to asset $`j`$. Once fixed, the target
intensity for asset $`j`$ is $`f_{\tau,j} = \tilde{f}_{\tau,\pi(j)}`$. Assets
assigned to dummy slots ($`\pi(j) > m`$) have $`f_{\tau,j} = 0`$ and are
excluded from the portfolio.

**Problem formulation.** We formulate a linear assignment problem to choose
$`\pi`$. For $`k \in \{2,\ldots,T\}`$, define the total return of asset $`j`$
over the interval $`[\tau_{k-1}, \tau_k)`$ as

```math
r_{k,j} = \frac{p_{\tau_k,j} + \delta_{k,j}}{p_{\tau_{k-1},j}} - 1,
```

where $`p_{\tau,j}`$ is the BBO midprice at rebalance time $`\tau`$ and
$`\delta_{k,j}`$ is the per-share dividend if an ex-dividend date for asset
$`j`$ falls within the interval, otherwise zero. All values are split-adjusted
to the end of the backtest period. We define the per-asset spread cost at
$`\tau_k`$ as

```math
\kappa_{k,j} = \frac{a_{\tau_k,j} - b_{\tau_k,j}}{a_{\tau_k,j} + b_{\tau_k,j}}.
```

This measures the half-spread as a fraction of the midprice.

Define the *slot weight* as

```math
w_{\tau,i} = \frac{\tilde{f}_{\tau,i}}{A_\tau},
```

representing the fraction of liquid NAV allocated to slot $`i`$. The
pre-rebalance weight drifts with returns, giving

```math
w_{\tau_k^-,i} \approx w_{\tau_{k-1},i} (1 + r_{k,j}).
```

Define the per-period utility for assigning asset $`j`$ to slot $`i`$ at time
$`\tau_k`$ as

```math
u_{k,ji} = s_k \, w_{\tau_{k-1},i} \, r_{k,j} - |w_{\tau_k,i} - w_{\tau_{k-1},i} (1 + r_{k,j})| \, \kappa_{k,j},
```

where $`s_k \in [-1, +1]`$ is a narrative multiplier encoding whether interval
$`k`$ should contribute positively or negatively to the profit-and-loss
trajectory. The narrative multipliers are loaded from a precomputed file
aligned with market regimes (see section 5). Note that transaction costs are
always subtracted regardless of $`s_k`$, preventing the optimizer from playing
donation simulator with market makers to maximize spreads when targeting
losses. The total utility for each $`(j,i)`$ pair is

```math
U_{ji} = \sum_{k=2}^T u_{k,ji}.
```

**Forced assignments.** Certain high-profile assets (*e.g.*, AAPL, NVDA, TSLA)
are forced to specific pixels in the upper-left corner for visual
recognizability. Let $`\mathcal{F} \subset [n] \times [m]`$ denote the set of
forced $`(j,i)`$ pairs.

**Optimization.** The assignment problem is

```math
\begin{array}{ll}
\text{maximize} & \sum_{j=1}^n \sum_{i=1}^n U_{ji} \Pi_{ji} \\
\text{subject to} & \Pi \in \{0,1\}^{n \times n} \text{ is a permutation matrix}, \\
& \Pi_{ji} = 1 \text{ for all } (j,i) \in \mathcal{F},
\end{array}
```

with variable $`\Pi`$. We solve this by removing forced symbols and pixels from
the optimization, solving the reduced linear assignment problem, then
reinserting forced pairs. The reduced problem is efficiently solvable using the
Jonker–Volgenant algorithm (the default option in SciPy's
`linear_sum_assignment`).

**Padding.** The dummy slots $`i > m`$ (always zero weight) allow us to
represent the assignment as a square permutation matrix $`\Pi \in \{0,1\}^{n
\times n}`$. Assets mapped to dummy slots have zero exposure for all $`\tau`$
and are excluded from the portfolio.

## 5. Data sources and processing

**Data sources.** I used the [iconic
video](https://www.youtube.com/watch?v=FtutLA63Cp8) uploaded by kasidid2 as a
source for Bad Apple. I used Databento for raw price data in two schemas:
BBO-1s, which provides the best bid and ask prices for every second (in a
sparse format; seconds with no order book updates do not appear in the data),
and OHLCV-1d, which provides daily open, high, low, close, and volume. The
BBO-1s data is used for portfolio valuation and spread estimation and the
OHLCV-1d data is used for verifying corporate action dates when the effective
date was provided but not the ex date. I used LSEG for corporate actions data
(dividends and adjustment factors).

**Time range and rebalance periods.** I considered the time range of 2024-12-10
to 2026-01-01 (exclusive). I took $`T=6572`$ (the number of frames in the Bad
Apple video played at 30 fps), and constructed the $`\tau_k`$ as follows. The
first rebalance of each trading day occurs at 9:45 AM Eastern, 15 minutes after
market open, with the exception of the very first day of the simulation, in
which I first rebalanced at 11:00 AM Eastern. I then rebalanced every 15
minutes, with the last rebalance for the day occurring 15 minutes before market
close. On regular trading days, the last rebalance is at 3:45 PM Eastern, while
on half days (1:00 PM close), the last rebalance is at 12:45 PM Eastern. This
is to ensure adequate liquidity (spreads widen significantly at open and
close).

**Asset universe.** I fetched all symbols in the `XNAS.ITCH` dataset from
Databento for the time range 2024-12-10 to 2026-01-01 (exclusive). I filtered
to instrument class `K` (stocks) and excluded symbols ending with special
suffixes `W` (warrants), `R` (rights), `U` (units), `+` (warrants), `=`
(units), and `^` (test/special symbols). I additionally filtered out the name
`MTEST` (which probably shouldn't have been returned anyway, or should have
been given the suffix `^`). This resulted in a list of approximately 11,499
potential names.

I then applied a further filter to exclude names that are either not available
in LSEG, or for which my barebones parsing didn't correctly map the ticker to
the RIC. I applied one final filter to include only assets with complete price,
spread, and corporate action observations across all trading intervals. This
*does* remove assets that simply underwent a name change, but for my purposes I
had more than enough assets to complete the project.

Additionally, I filtered out assets that do not have complete OHLCV-1d data
across all trading days. This is necessary because the corporate action date
verification algorithm (see below) requires daily open/close prices to verify
when an adjustment actually took effect. Assets are also dropped if LSEG
reports a corporate action with only an effective date (no ex-date) and the
verification algorithm cannot find a matching price discontinuity. After all
these filters, I had 5,180 eligible securities for assignment. (This is also
just good hygiene, since if there is not an OHLCV entry for a day, then that
means it literally had no trades on that day, so it has terrible liquidity.)

**Raw price data aggregation.** Since BBO-1s records exist only when there are
changes to the top level of the order book within a second, I aggregated the
data into two dense formats with different sample intervals.

*15-minute BBO* is used for rebalancing. At each rebalance time $`\tau`$, I
looked up the most recent BBO record to obtain the bid and ask prices
$`b_{\tau,j}`$ and $`a_{\tau,j}`$, and hence the midprice $`p_{\tau,j}`$. The
Databento field `ts_recv` marks the end of the 1-second interval, so to get
$`a_{\tau}`$ and $`b_{\tau}`$ I just use the last BBO observation within the
day, up to *and including* $`\tau`$.

*1-minute BBO* is used for intra-interval valuation. This is computed in the
same way as the 15-minute BBO, but with 1-minute aggregation instead of 15. The
resampling ends at 3:59 PM (or 12:59 PM on half days), one minute before close,
which avoids close-of-day spread widening artifacts.

**Corporate action adjustments.** All prices are split-adjusted to the end of
the backtest period. LSEG reports each corporate action with an *adjustment
factor* $`\phi > 0`$. A factor $`\phi < 1`$ usually indicates a forward split
(*e.g.*, $`\phi = 0.5`$ for a 2-for-1 split), while $`\phi > 1`$ usually
indicates a reverse split. There are other types of adjustments (*e.g.*,
spin-offs) which I treat in the same way. To adjust historical prices to
post-split basis, I multiplied all prices before the adjustment date by
$`\phi`$.

*LSEG factors and date fields.* LSEG provides two date fields for corporate
actions: `TR.CAExDate` (ex-date) and `TR.CAEffectiveDate` (effective date).
Neither field reliably indicates the actual date of the price discontinuity.
Effective dates can fall on weekends or holidays, and both ex-dates and
effective dates can differ from the actual adjustment date by multiple days.
Additionally, LSEG filters corporate action queries by announcement date rather
than effective date, so adjustments announced before the query start date may
be omitted even if they take effect within the query period. I extended the
query start date to 2020-01-01 in an effort to catch all such cases.

*Date verification.* For every adjustment within the simulation period, I
verify the date by searching for the overnight price discontinuity that matches
the expected factor. For large adjustments ($`\phi > 2`$ or $`\phi < 0.5`$), I
search up to 30 trading days from the LSEG-reported date. For small
adjustments, I search only 4 days to avoid false matches. For each candidate
$`d_k`$, I compute the overnight return

```math
r_k = \frac{o_{d_k,j}}{c_{d_{k-1},j}}
```

using OHLCV-1d data, where $`o_{d,j}`$ and $`c_{d,j}`$ denote the daily open
and close prices for asset $`j`$ on trading day $`d`$. I accept $`d_k`$ as the
adjustment date if

```math
|\log r_k - \log \phi| < 0.2.
```

If no candidate satisfies this tolerance, the symbol is dropped from the
backtest entirely. I also exclude assets without complete OHLCV-1d data across
all trading days, since the verification algorithm requires daily prices.

The raw LSEG data (with date type annotations) is stored in
`config/splits_lseg.json`. The verified adjustment dates are stored in
`config/splits.json`. The list of OHLCV-complete symbols (minus any dropped due
to unverifiable adjustments) is stored in `config/ohlcv_complete_symbols.json`.

*Price adjustment.* When loading prices for optimization or backtest, I applied
adjustments on-the-fly. For asset $`j`$ with adjustment factor $`\phi`$
effective at time $`t^*`$, I transformed all prices $`p_{t,j}`$ with $`t <
t^*`$ to $`\phi p_{t,j}`$. If asset $`j`$ has multiple adjustments with factors
$`\phi_1, \ldots, \phi_K`$ effective at times $`t_1^* < \cdots < t_K^*`$, then
the adjustments compound in the sense that prices before $`t_1^*`$ are
multiplied by $`\prod_{k=1}^K \phi_k`$.

*Dividend adjustment.* Dividends declared before a corporate action but paid
after must also be adjusted. For a dividend with ex-date $`d_{\text{ex}}`$ and
amount $`\delta`$, I multiplied by each adjustment factor $`\phi_k`$ whose
effective date $`t_k^*`$ satisfies $`d_{\text{ex}} < t_k^*`$. In other words,
dividends before multiple adjustments are scaled by the compound factor.
Adjusted dividends are stored in `config/dividends_adjusted.json`.

**Storyline.** The narrative coefficients $`s_k`$ were determined through a
combination of manual specification and sampling. I divided the year into 8
periods of roughly equal duration, each corresponding to what I perceived to be
a distinct market sentiment regime. For each period, I specified an interval of
allowable sentiment values $`[s_{\min}, s_{\max}]`$. I then generated 128
samples within the resulting hyperrectangle using a Sobol' sequence, ran each
through the assignment optimization and backtest, and plotted the resulting NAV
trajectories. The final coefficients were selected from the sample whose
trajectory best matched the image in my head.
