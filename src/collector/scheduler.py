# scheduler.py
from __future__ import annotations
import time
import argparse
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import yaml
from datetime import datetime, timezone

from collector import run_collector

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def load_yaml(path: Path) -> dict:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return {}

def should_run(cfg: dict, last_run_ts: float) -> bool:
    """Determines if the job is due based on interval_seconds."""
    sched = cfg.get("schedule", {})
    interval = sched.get("interval_seconds")
    
    if not interval:
        return True # Run once? Or never? Assuming run immediately if no interval.
        
    if last_run_ts is None:
        return True
        
    return (time.time() - last_run_ts) >= int(interval)

def tick(configs_dir: Path, last_runs: dict, pool: ThreadPoolExecutor):
    """Scan YAMLs and submit tasks to thread pool."""
    for p in sorted(configs_dir.glob("*.yaml")):
        cfg = load_yaml(p)
        if not cfg: continue

        # Check schedule
        p_str = str(p)
        if should_run(cfg, last_runs.get(p_str)):
            print(f"Triggering {p.name}...")
            # Submit to pool
            pool.submit(safe_run, cfg, p_str)
            last_runs[p_str] = time.time()

def safe_run(cfg: dict, name: str):
    """Wrapper to catch exceptions inside threads."""
    try:
        run_collector(cfg)
    except Exception as e:
        print(f"ERROR running {name}: {e}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--configs-dir", default="src/configs")
    ap.add_argument("--tick-seconds", type=int, default=5)
    args = ap.parse_args()
    
    configs_dir = Path(args.configs_dir).expanduser()
    last_runs = {} # path -> timestamp
    
    # Thread pool for concurrency (prevent network I/O blocking)
    with ThreadPoolExecutor(max_workers=5) as pool:
        while True:
            tick(configs_dir, last_runs, pool)
            time.sleep(args.tick_seconds)

if __name__ == "__main__":
    main()