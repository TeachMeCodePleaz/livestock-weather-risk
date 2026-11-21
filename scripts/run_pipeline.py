#!/usr/bin/env python3
"""Run 4-stage PySpark pipeline on synthetic demo or previously downloaded NOAA files."""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from livestock_weather.settings import load_settings
from livestock_weather.pipeline import run_pipeline
from livestock_weather.visuals import plot_ranking


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo", action="store_true", help="Create+use SYNTHETIC example data")
    parser.add_argument("--raw-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    raw_dir = Path(args.raw_dir) if args.raw_dir else ROOT / "data" / ("demo_raw" if args.demo else "raw")
    if args.demo:
        from make_demo_data import create_demo
        create_demo(raw_dir)
    output_dir = Path(args.output) if args.output else ROOT / "data" / ("demo_processed" if args.demo else "processed")
    cfg_file = args.config or ROOT / "config" / ("demo.json" if args.demo else "settings.json")
    cfg = load_settings(cfg_file)
    candidates = list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.csv.gz"))
    if not candidates or not (raw_dir / "ghcnd-stations.txt").exists():
        parser.error("Missing NOAA inputs. Use --demo or scripts/download_noaa.py")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("LivestockWeatherRisk").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        results = run_pipeline(spark, str(raw_dir / "*.csv*"),
                               raw_dir / "ghcnd-stations.txt", output_dir, cfg)
        print("=== Relative state ranking: lower = less historical weather exposure ===")
        results["scoring"].select("state", "overall_score", "rank").show(60, truncate=False)
        if results["scoring"].limit(1).count():
            png = plot_ranking(results["scoring"], ROOT / "reports/figures" / ("demo_state_ranking.png" if args.demo else "state_ranking.png"))
            print("Chart:", png)
        else:
            print("No eligible states. Inspect data completeness and thresholds.")
        print("Parquet outputs:", output_dir.resolve())
        if args.demo:
            print("IMPORTANT: All displayed demo results are SYNTHETIC, NOT NOAA findings.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
