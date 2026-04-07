"""
engine/__init__.py — Public interface for the QuantStand options scoring engine.

External code calls exactly one function: run_screener().
Nothing else in this package is part of the public API.
"""

import logging
from typing import Optional

import psycopg2.pool
import yaml

from .screener import OptionScreener

log = logging.getLogger(__name__)


def run_screener(
    config_path: str = "config/config.yaml",
    underlying_ids: Optional[list] = None,
) -> list:
    """
    Load config, open database connection pool, run the screener across all
    active underlyings (or the supplied underlying_ids list), return a ranked
    list of ScoredContract objects sorted by composite_score descending.

    Both passing and failing contracts are included in the return value.
    Callers filter by scored.passed_all_filters if they want only trade signals.

    Args:
        config_path:    Path to config.yaml (relative to cwd or absolute).
        underlying_ids: Optional list of underlying_id ints to restrict the run.
                        If None, all active underlyings in the DB are used.

    Returns:
        list[ScoredContract] sorted by composite_score descending.

    Raises:
        RuntimeError: if the database connection pool cannot be created.
        FileNotFoundError: if config_path does not exist.
        yaml.YAMLError: if config_path is not valid YAML.
    """
    config = _load_config(config_path)
    pool = _create_connection_pool(config["database"])
    try:
        screener = OptionScreener(pool, config)
        return screener.run(underlying_ids)
    finally:
        pool.closeall()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _load_config(config_path: str) -> dict:
    """
    Load and return the YAML config file.

    Raises FileNotFoundError or yaml.YAMLError on failure — both are fatal.
    """
    with open(config_path, "r") as fh:
        config = yaml.safe_load(fh)
    log.debug("Config loaded from %s", config_path)
    return config


def _create_connection_pool(db_cfg: dict) -> psycopg2.pool.ThreadedConnectionPool:
    """
    Create a psycopg2 ThreadedConnectionPool.

    Raises RuntimeError (wrapping the original psycopg2 error) if connection fails
    so that the caller receives a clear, consistent exception type.
    """
    try:
        pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=db_cfg["host"],
            port=db_cfg["port"],
            dbname=db_cfg["name"],
            user=db_cfg["user"],
            password=db_cfg["password"],
        )
        log.debug(
            "DB connection pool created — %s:%s/%s",
            db_cfg["host"], db_cfg["port"], db_cfg["name"],
        )
        return pool
    except psycopg2.Error as exc:
        raise RuntimeError(
            f"Failed to connect to qs_options DB at "
            f"{db_cfg['host']}:{db_cfg['port']}/{db_cfg['name']}: {exc}"
        ) from exc
