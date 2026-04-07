#!/usr/bin/env python3
"""
run_paper_trader.py — PM2 entry point for the paper trading simulator.

Equivalent to `python3 -m simulator` but invocable as a plain script.
Inserts the repo root into sys.path so the `simulator` and `engine`
packages resolve correctly when PM2 launches the file directly.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from simulator import run_paper_session

if __name__ == "__main__":
    run_paper_session()
