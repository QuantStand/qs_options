# qs_options — QuantStand Options Engine

Options scoring, paper trading simulation, ML optimisation, and live execution
for the QuantStand options strategy (cash-secured puts on equity and ETF underlyings).

---

## What this repo is

This repo contains the strategy and execution layer of the QuantStand options engine.
It reads from the `qs_options` database (populated by the data collection layer in
[qs-data](https://github.com/QuantStand/qs-data)) and implements:

- A rules-based scoring engine that ranks put-selling opportunities
- A paper trading simulator that executes paper trades from scoring output
- An ML optimisation layer that learns optimal threshold parameters from trade history
- A live execution bridge to IBKR via ib_insync

These four layers are built sequentially. See the development status table below.

---

## Relationship to qs-data

`qs-data` owns all data collection and database infrastructure.
`qs_options` is a consumer of that infrastructure — it reads, never writes to,
the data collection tables.

| Concern | Repo |
|---|---|
| Database schema | qs-data (`schema/options/schema.sql`) |
| Data collection jobs | qs-data (`connectors/ibkr/options/`) |
| Scoring engine | **qs_options** (`engine/`) |
| Paper trading simulator | **qs_options** (`simulator/`) |
| ML optimisation | **qs_options** (`ml/`) |
| Live execution | **qs_options** (`execution/`) |

---

## Databases

| Database | Engine | Purpose |
|---|---|---|
| `qs_options` | PostgreSQL 14 | Options chains, vol surface, trade log, underlyings |
| `qs_1min_ohlcv` | TimescaleDB | 1-minute OHLCV candles (12 instruments) — owned by qs-data |

The scoring engine reads from `qs_options`. It does not use `qs_1min_ohlcv` directly.

---

## Port reference

| Gateway | Host | Port |
|---|---|---|
| Live IB Gateway | 127.0.0.1 | 4002 |
| Paper IB Gateway | 127.0.0.1 | 4003 |
| TWS Live | 127.0.0.1 | 7496 |
| TWS Paper | 127.0.0.1 | 7497 |

All data collection connects to port 4002.
The paper trading simulator connects to port 4003.
Live execution connects to port 4002 — only when `trading.mode = LIVE` in config.yaml.

---

## Development status

| Component | Status |
|---|---|
| qs-data infrastructure | Live |
| qs_options database schema | Live |
| Data collection jobs | Live |
| Scoring engine | In development |
| Paper trading simulator | Not started |
| ML optimisation layer | Not started |
| Live execution bridge | Not started |

---

## Configuration

Copy `config/config.example.yaml` to `config/config.yaml` and populate with
real values before running any component.

```bash
cp config/config.example.yaml config/config.yaml
# edit config/config.yaml with real DB credentials and IBKR settings
```

`config/config.yaml` is listed in `.gitignore` and must never be committed.
Real database credentials and IBKR account details must not appear in this repository.

---

## Repository structure

```
qs_options/
├── engine/          ← Scoring engine (rules-based put ranking)
├── simulator/       ← Paper trading simulator
├── ml/              ← ML optimisation layer (Bayesian threshold tuning)
├── execution/       ← Live execution bridge (ib_insync order routing)
├── config/
│   └── config.example.yaml
└── docs/
    └── trading/
        └── position_management_rules.md   ← Pre-agreed rules for all open positions
```

---

## Position management

Pre-agreed rules for managing all open positions are documented in
`docs/trading/position_management_rules.md`. This file is the source of truth
for position management decisions. No rule may be overridden in the moment —
changes require a documented update to that file before taking effect.

---

## Known issues & fixes

### 2026-04-10 — NULL `open_interest` crash in `engine/screener.py`

**Symptom:** `screener.run()` failed with `TypeError: int() argument must be a
string, a bytes-like object or a real number, not 'NoneType'` for every
underlying, producing 0 scored contracts and silencing all alerts.

**Root cause:** IBKR does not always return open interest data for options
contracts. When `open_interest` is NULL in `options_chain_snapshots`,
`load_contracts()` called `int(None)` which raised a TypeError.

**Fix:** `engine/screener.py` line 203 — added None-guard consistent with the
existing pattern used for `bid`, `ask`, `gamma`, and `vega`:
```python
# Before
open_interest=int(open_interest),
# After
open_interest=int(open_interest) if open_interest is not None else 0,
```
Contracts with `open_interest=0` are correctly filtered out by the
`min_open_interest` threshold in `apply_filters()`.

**Tests:** `tests/test_screener.py` — `TestLoadContractsNullOpenInterest`
