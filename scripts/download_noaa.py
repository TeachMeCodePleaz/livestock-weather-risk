#!/usr/bin/env python3
"""Opt-in download of REAL NOAA GHCN-Daily yearly CSVs and station metadata.

Large files! Start with one year; data are intentionally not in the ZIP or Git.
"""
import argparse
from pathlib import Path
from urllib.request import urlretrieve

BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"


def download(url, target, overwrite=False):
    if target.exists() and not overwrite:
        print(f"Skipping existing: {target}")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} -> {target}")
    tmp = target.with_name(target.name + ".part")
    try:
        urlretrieve(url, tmp)
        tmp.replace(target)
    finally:
        tmp.unlink(missing_ok=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--years", type=int, nargs="+", required=True)
    p.add_argument("--output", default="data/raw")
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()
    if any(y < 1700 or y > 2025 for y in args.years):
        p.error("Choose years within the proposal scope (<= 2025)")
    out = Path(args.output)
    download(f"{BASE}/ghcnd-stations.txt", out / "ghcnd-stations.txt", args.overwrite)
    for year in sorted(set(args.years)):
        download(f"{BASE}/by_year/{year}.csv.gz", out / f"{year}.csv.gz", args.overwrite)
