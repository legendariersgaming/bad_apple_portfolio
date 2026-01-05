# Commentary

This document contains personal notes from building the Bad Apple portfolio
project. For the technical specification, see [TECHNICAL.md](TECHNICAL.md).

## On procrastination

I had this idea in September 2025, but I didn't actually get to working on it
until the last week of December, as is customary. This meant I didn't have all
that much time to iron out the details. I also spent an unfortunate amount of
money on this project and it would've been maybe ten times as expensive if I
had gone full fidelity. It also would've been a significantly greater
engineering challenge since that is a LOT of data to process, and I simply
didn't have the time to do that.

## On data pipelines

I want to take a moment to admonish you not to take what I have done as a model
for how this sort of thing ought to be done. The pipeline outlined here is the
result of multiple rounds of lowering my expectations and running out of budget
and time to simplify places that were overcomplicated. The result is messy and
inefficient. My approaches would not have scaled since I kinda did the dumbest
thing I could think of at every processing step, some of which should probably
be skipped in a smarter pipeline anyway, like the forward-filling. If you are
embarking on a finance project, then you should carefully consider what you are
trying to do. You can do better.

## On financial data in general

Wrangling financial data is truly nightmarish. If you're considering a similar
project, then budget extra time for data cleaning and verification. Then budget
extra time on top of that extra time, and repeat this until you have as much
extra time as you have data. You will need it.

I wish I could have used Databento for everything, but their corporate actions
data was too expensive for this project (which I am not monetizing).

If you're familiar with the available Databento schema, then you might be
wondering why I didn't use BBO-1m data instead of BBO-1s. The answer is that I
am dumb. I originally wanted to track intraperiod valuations down to the
second, until I realized just how much data this actually was and how long a
single backtest would take. But I had already paid for BBO-1s data, so I used
it.

Due to a bug in the data pipeline, I only resampled up to 3:59 PM (1 minute
before close) instead of 4:00 PM. I didn't feel like re-running the whole
pipeline to fix this. Spreads are terrible at close anyway so I don't think the
midprice then is all that useful. This is probably better for visualization
purposes (no sudden jumps at close due to liquidity evaporation).

## On corporate actions data

**Action types.** I could not figure out from LSEG's API what *kind* of
corporate actions each event was (*e.g.* just a stock split, a spin-off, a
merger or acquisition, maybe just a price correction?), so probably for some of
these I should not have adjusted the shares (or maybe even the price). It's
surely a small effect in the overall video so I hope I may be forgiven.

**Adjustment factors.** I have no idea what all goes into LSEG's adjustment
factors. I could not find the relevant API documentation, and the adjustment
types (`Capital Change Type`, `Refinitiv Pricing Only`, etc.) are not clearly
defined. Some adjustments are clearly stock splits. Others appear to be
spin-off adjustments (*e.g.*, GE Vernova). Some have very strange metadata
(effective dates years in the past or future, terms that don't match the
factor). After hours of investigation, I gave up trying to understand the
semantics and just applied all adjustments with $`\phi \neq 1`$.

**Adjustment dates.** Besides understanding the semantics, I also had a hell of
a time trying to understand *when* the adjustments were supposed to apply. LSEG
provides two date fields that seem relevant: `TR.CAExDate` (ex-date) and
`TR.CAEffectiveDate` (effective date). I could not find documentation for what
these fields actually mean.

I originally only knew about `TR.CAEffectiveDate`. This caused immediate
problems since effective dates apparently can fall on weekends and holidays,
and dates could appear multiple days, and sometimes *weeks* before the actual
price discontinuity—dates that made no sense at all. I implemented a jump
discontinuity detection algorithm to search for the actual split date. This
helped, but phantom gains kept appearing in names like MULN, then LTL, then
ATCH, then DGLY. Some others also caused problems but I forget their names.

ATCH was interesting since it had a 60:1 reverse split on January 2, 2025.
But when I queried LSEG for corporate actions from 2024-12-10 to 2026-01-01,
ATCH was missing entirely. It seems that LSEG filters by *announcement date*,
not effective date. Since ATCH's split was announced before December 10, it was
omitted from my query. I ended up extending my query start date back to
2020-01-01 to catch these. (Maybe I should have gone back even further?)

DGLY was producing an 8,000% phantom gain. While investigating, I discovered
the `TR.CAExDate` field. DGLY had a 100:1 reverse split in May 2025. The
effective date was 2025-05-07, but the actual price change was sixteen days
later on 2025-05-23. The ex-date, however, correctly showed 2025-05-23. I
assumed the ex-date was the reliable field and the effective date was...
something else. DGLY also had a *separate* 20:1 adjustment with ex-date
2025-05-07 and effective date 2025-09-01 (which is *in the future* relative to
the ex-date). I still have no idea what this means.

Well it turns out `TR.CAExDate` isn't reliable either. SQQQ and UVXY had 5:1
reverse splits that took effect 2025-11-20, but their ex-dates were listed as
2025-11-18. AMRN and STEM also had mismatches. So I was still seeing phantom
gains upwards of 300%.

This is all to say that I had to extend jump discontinuity detection to the ex
date also.

## On the storyline

To determine the market regimes used for creating the storyline, I kinda just
made something up. I didn't want the profit-and-loss curve to go monotonically
up or down, and I had a general idea of the overall market sentiment throughout
the year that I wanted to match.

## Miscellaneous notes

If anyone knows how to fix the rendering bug for some of the inline math where
MathJax doesn't give enough space for the whole expression so it ends up
wrapping lines, please let me know!

ALSO, if anybody has the data to do a full-fidelity simulation like this and
wants to reproduce this (but better) then let me know. I would find it
interesting. But I am also not sure how much more financial data wrangling I
can take.
