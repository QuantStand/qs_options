"""
models.py — PaperPosition dataclass for the QuantStand paper trading simulator.

Pure data only. No logic, no database calls, no imports beyond dataclasses,
datetime, and typing.
"""

from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional


@dataclass
class PaperPosition:
    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------
    trade_id: int                    # populated from DB RETURNING on INSERT
    underlying_id: int
    symbol: str

    # ------------------------------------------------------------------
    # Contract definition
    # ------------------------------------------------------------------
    expiry_date: date
    strike: float
    option_type: str                 # always 'P'
    contracts: int                   # always 5 for now — see config

    # ------------------------------------------------------------------
    # Entry state
    # ------------------------------------------------------------------
    entry_time: datetime             # UTC
    entry_price: float               # mid at entry
    entry_underlying_price: float
    entry_delta: float
    entry_theta: float
    entry_iv: float
    entry_iv_percentile: float
    entry_dte: int
    entry_composite_score: float
    entry_ann_roi: float
    premium_collected: float         # entry_price * 100 * contracts
    collateral_assigned: float       # strike * 100 * contracts

    # ------------------------------------------------------------------
    # Threshold parameters used at entry — stored for ML training
    # ------------------------------------------------------------------
    threshold_min_score: float
    threshold_min_roi: float
    threshold_min_pop: float
    threshold_min_oi: int

    # ------------------------------------------------------------------
    # Current state (updated on each monitoring cycle, in-memory only)
    # ------------------------------------------------------------------
    current_price: float
    current_underlying_price: float
    current_delta: float
    current_dte: int
    unrealised_pnl: float            # (entry_price - current_price) * 100 * contracts

    # ------------------------------------------------------------------
    # Exit state (None while open)
    # ------------------------------------------------------------------
    exit_time: Optional[datetime]
    exit_price: Optional[float]
    exit_reason: Optional[str]       # EXPIRED | EARLY_CLOSE | ASSIGNED | STOP_LOSS | MANUAL
    realised_pnl: Optional[float]
    pnl_pct_of_premium: Optional[float]   # realised_pnl / premium_collected
    was_assigned: bool
    assigned_cost_basis: Optional[float]
