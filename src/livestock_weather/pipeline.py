"""Compose independently developed stages; data contracts are documented in README."""
from pathlib import Path
from .etl import read_stations, read_observations, clean_station_days
from .analytics import state_year_metrics, state_summary, state_seasonal_metrics
from .scoring import score_states


def run_pipeline(spark, raw_glob, stations_path, output_dir, cfg):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    raw = read_observations(spark, raw_glob)
    stations = read_stations(spark, stations_path)
    daily = clean_station_days(raw, stations)
    annual = state_year_metrics(daily, cfg)
    summary = state_summary(annual)
    seasonal = state_seasonal_metrics(daily, cfg)
    scoring = score_states(summary, cfg)
    # For a production run, Spark will write directories of Parquet part files.
    daily.write.mode("overwrite").partitionBy("year").parquet(str(output / "clean_daily.parquet"))
    annual.write.mode("overwrite").parquet(str(output / "state_annual.parquet"))
    summary.write.mode("overwrite").parquet(str(output / "state_summary.parquet"))
    seasonal.write.mode("overwrite").parquet(str(output / "state_seasonal.parquet"))
    scoring.write.mode("overwrite").parquet(str(output / "state_ranking.parquet"))
    return {"daily": daily, "annual": annual, "summary": summary,
            "seasonal": seasonal, "scoring": scoring}
