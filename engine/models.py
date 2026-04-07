"""
models.py — Pure data structures for the QuantStand options scoring engine.

No logic, no database calls, no imports beyond dataclasses and datetime.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional


@dataclass
class OptionContract:
    """
    Represents one row from options_chain_snapshots joined with volatility_surface.
    All fields typed. No defaults — every field must be supplied at construction.
    """

    underlying_id: int
    symbol: str
    snapshot_time: datetime          # UTC
    underlying_price: float          # Stock price at snapshot
    expiry_date: date
    dte: int                         # Pre-computed — do not recalculate
    strike: float
    option_type: str                 # Always 'P' — puts only
    bid: float
    ask: float
    mid: float                       # Stored — do not recompute
    open_interest: int
    implied_vol: float               # Decimal: 0.746 = 74.6%
    delta: float                     # Negative for puts — scorer takes abs()
    gamma: float
    theta: float                     # Negative — scorer takes abs()
    vega: float
    iv_percentile_52w: Optional[float]   # None if < 252 days history
    iv_percentile_30d: Optional[float]   # Fallback. None if < 30 days
    vix_level: Optional[float]           # Macro context
    ibkr_conid: int                  # Option contract conid for order routing


@dataclass
class ScoredContract(OptionContract):
    """
    Extends OptionContract with all computed scoring fields.

    Scoring fields are populated by scorer.py.
    filter_failures and passed_all_filters are populated by screener.py.

    All added fields have defaults so that ScoredContract can be constructed
    from an OptionContract via ScoredContract(**vars(contract), ...).
    """

    composite_score: float = 0.0
    annualised_roi: float = 0.0
    theta_per_collateral: float = 0.0
    prob_profit: float = 0.0
    collateral: float = 0.0
    premium_per_contract: float = 0.0
    total_premium_5_contracts: float = 0.0
    effective_entry_if_assigned: float = 0.0
    passed_all_filters: bool = False
    filter_failures: list = field(default_factory=list)
