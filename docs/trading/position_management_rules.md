# Position Management Rules
**QuantStand — Eterne Group Pte Ltd**
Last updated: 2026-04-07

---

## Purpose

This document defines pre-agreed rules for managing open options positions.
All rules are defined before the trade is placed or immediately after entry.
No rule may be overridden in the moment — changes require a documented update
to this file before they take effect.

---

## Active Position: VRT Apr24'26 245 Put

**Entry date:** April 6, 2026
**Contracts:** 5
**Strike:** $245
**Expiry:** April 24, 2026
**Fill price:** $10.74 per share
**Premium collected:** $5,370 USD
**Collateral assigned:** $122,500 USD (SGOV)
**Effective cost basis if assigned:** $234.25 per share
**Breakeven:** $234.26
**Probability of profit at entry:** 75%
**IV percentile at entry:** 92nd
**DTE at entry:** 18
**IBKR Order ID:** 667769626

---

## Rule 1 — Early close on theta decay

Check the position daily at 10:00am US Eastern time.

**Before April 15:**
If the ask to buy-to-close the position is at or below **$2.69**
(25% of original premium remaining — 75% profit captured),
place a limit buy-to-close order at **$2.50**.
Close within that same session. Do not chase if unfilled — reprice once.

**April 15 to April 18:**
Threshold tightens due to approaching earnings event.
If the ask to buy-to-close is at or below **$4.30**
(40% of original premium remaining — 60% profit captured),
close immediately at market.

**Rationale:** The early close threshold is time-decaying because VRT reports
Q1 2026 earnings on April 22 — two days before expiry. In the final days
before a binary event, gamma risk accelerates faster than the remaining theta
reward justifies. Closing earlier than pure theta math would suggest is the
correct risk-adjusted decision.

---

## Rule 2 — Pre-earnings hard close

**If Rule 1 has not triggered by end of day April 18:**
Close the entire position at market open on **April 19**.
Accept any price. Do not negotiate for a better fill.
Do not hold through the April 22 earnings event under any circumstances.

**Rationale:** VRT Q1 2026 earnings are on April 22. With 2 days to expiry
at that point, gamma is at maximum. A bad earnings print could gap VRT through
$245 before we can react. The residual premium available (estimated $1–2 by
April 19) does not justify this tail risk.

---

## Rule 3 — Stop loss / reassessment trigger

If VRT closes below **$250.00** on any trading day before April 19,
do not close automatically. Stop and reassess immediately using
the following checklist:

- What is the current delta? (Above 0.40 = elevated assignment risk)
- How many DTE remain?
- What is the buy-to-close cost relative to premium collected?
- Has anything changed in the fundamental thesis for VRT?

If delta has exceeded 0.40 and DTE is below 10, close the position.
If delta is below 0.40, hold and continue monitoring daily.

**Rationale:** $250 gives a $5 buffer above the strike. This is not an
emergency but it requires active attention rather than passive monitoring.

---

## Rule 4 — Assignment acceptance

If the position expires with VRT below $245 and we are assigned:

- Accept the 500 shares at $234.25 effective cost basis per share.
- Do not panic sell.
- This was always the intended Tranche 1 entry price for VRT equity.
- Transition the shares to the equity position monitoring process.
- Begin evaluating a covered call strategy on the assigned shares
  for the next available expiry.

**Rationale:** Assignment at $234.25 is not a loss scenario — it is the
deliberate entry price designed from the beginning. The $5,370 premium
collected permanently reduces our cost basis regardless of outcome.

---

## General Rules — All Future Positions

The following rules apply to every options position opened by QuantStand.

### Early close framework (no earnings event in expiry window)
Close when 25–35% of original premium remains (65–75% profit captured).
This is the empirically validated range for optimal Sharpe ratio on
short-dated put-selling strategies.

### Early close framework (earnings event within expiry window)
Apply the time-decaying threshold as defined in Rule 1 above.
Always be fully closed before the earnings announcement session.
Never hold a short put through an earnings event with less than 5 DTE.

### Assignment policy
Assignment is always an acceptable outcome if the strike price equals
or is below the pre-defined target entry price for that underlying.
Assignment is never an acceptable outcome if the strike was set
without a corresponding equity entry thesis.

### Maximum loss per position
No single options position may represent more than 15% of total portfolio NAV
in collateral terms. Current portfolio NAV: approximately $2.35M USD.
Maximum collateral per position: $352,500 USD.

### Minimum IV percentile to open new position
Do not open new cash-secured put positions when the underlying's
52-week IV percentile is below 70.

### Minimum composite score to open new position
Do not open new positions where the composite scoring engine score
is below 0.60. Preferred threshold: 0.70 and above.

### Maximum concurrent open positions
During the paper trading and early live trading phase (first 6 months):
maximum 3 simultaneous open put positions across all underlyings.
This limit exists to ensure each position receives adequate manual monitoring
attention before the automated monitoring system is fully operational.

---

## Change log

| Date | Change | Reason |
|---|---|---|
| 2026-04-07 | Document created | First live trade entered — VRT Apr24 245 Put |
