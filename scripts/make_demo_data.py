#!/usr/bin/env python3
"""Generate tiny, wholly SYNTHETIC NOAA-shaped files. NOT real weather evidence."""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

STATIONS = [
    ("USW00000001", 30.20, -97.70, "TX", "SYNTHETIC TX A"),
    ("USW00000002", 32.80, -96.80, "TX", "SYNTHETIC TX B"),
    ("USW00000003", 41.60, -93.60, "IA", "SYNTHETIC IA A"),
    ("USW00000004", 42.50, -92.40, "IA", "SYNTHETIC IA B"),
    ("USW00000005", 43.10, -89.40, "WI", "SYNTHETIC WI A"),
    ("USW00000006", 44.00, -88.50, "WI", "SYNTHETIC WI B"),
]


def create_demo(out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stations = out / "ghcnd-stations.txt"
    with stations.open("w", encoding="utf-8") as f:
        for station, lat, lon, state, name in STATIONS:
            f.write(f"{station:<11} {lat:8.4f} {lon:9.4f} {200.0:6.1f} {state:2} {name:<30}\n")
        f.write(f"{'CA000000001':<11} {43.7:8.4f} {-79.4:9.4f} {200.0:6.1f} {'ON':2} {'SYNTHETIC NON-US':<30}\n")
    rng = random.Random(2026)
    for year in (2000, 2001):
        with (out / f"{year}.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for station, lat, lon, state, name in STATIONS:
                for month in (1, 4, 7, 10):
                    for day in range(1, 6):
                        d = date(year, month, day).strftime("%Y%m%d")
                        if state == "TX":
                            tmax = (38 if month == 7 else (14 if month == 1 else 27)) + rng.randrange(-4, 4)
                            tmin = (3 if month == 1 else 19) + rng.randrange(-3, 3)
                            rain = (55 if month == 10 and day < 4 else 2)
                        elif state == "IA":
                            tmax = (33 if month == 7 else (1 if month == 1 else 20)) + rng.randrange(-3, 4)
                            tmin = (-11 if month == 1 else 12) + rng.randrange(-3, 4)
                            rain = (61 if month == 4 and day < 4 else 5)
                        else:
                            tmax = (27 if month == 7 else (-2 if month == 1 else 16)) + rng.randrange(-3, 4)
                            tmin = (-17 if month == 1 else 9) + rng.randrange(-3, 4)
                            rain = (51 if month == 10 and day == 1 else 4)
                        for element, val in (("TMAX", tmax), ("TMIN", tmin), ("PRCP", rain)):
                            # Inject one rejected QC flag + one absent sentinel per year.
                            qflag = "X" if (station.endswith("1") and month == 7 and day == 1 and element == "TMAX") else ""
                            value = -9999 if (station.endswith("2") and month == 4 and day == 5 and element == "PRCP") else val * 10
                            writer.writerow([station, d, element, value, "", qflag, "Z", ""])
            # Should be eliminated by station metadata / US-only filter.
            writer.writerow(["CA000000001", f"{year}0701", "TMAX", 480, "", "", "Z", ""])
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/demo_raw")
    args = parser.parse_args()
    print(f"Synthetic data written to {create_demo(args.output).resolve()}")
