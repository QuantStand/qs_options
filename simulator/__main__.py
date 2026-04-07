"""
simulator/__main__.py — Entry point for `python -m simulator`

Called by PM2 when running the paper-trader process.
"""

from simulator import run_paper_session

if __name__ == "__main__":
    run_paper_session()
