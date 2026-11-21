"""Annual and seasonal extreme-weather exposure rates with observed-day denominators."""
from pyspark.sql import functions as F


def _station_group_metrics(daily, cfg, dimensions, minimum):
    """Compute per-station rates; each event is divided by valid observations of that element."""
    grouped = (
        daily.groupBy("station_id", "state", *dimensions)
        .agg(
            F.count("tmax_c").alias("heat_obs_days"),
            F.count("tmin_c").alias("cold_obs_days"),
            F.count("prcp_mm").alias("rain_obs_days"),
            F.sum(F.when(F.col("tmax_c") >= cfg["heat_c"], 1).otherwise(0)).alias("heat_event_days"),
            F.sum(F.when(F.col("tmin_c") <= cfg["cold_c"], 1).otherwise(0)).alias("cold_event_days"),
            F.sum(F.when(F.col("prcp_mm") >= cfg["heavy_rain_mm"], 1).otherwise(0)).alias("rain_event_days"),
        )
    )
    for metric, observation, events in (
        ("heat", "heat_obs_days", "heat_event_days"),
        ("cold", "cold_obs_days", "cold_event_days"),
        ("rain", "rain_obs_days", "rain_event_days"),
    ):
        grouped = grouped.withColumn(
            f"{metric}_rate",
            F.when(F.col(observation) >= minimum, F.col(events) / F.col(observation)),
        )
    return grouped


def state_year_metrics(daily, cfg):
    station_year = _station_group_metrics(daily, cfg, ["year"], cfg["min_obs_per_year"])
    return (
        station_year.groupBy("state", "year")
        .agg(
            *(F.avg(f"{m}_rate").alias(f"{m}_rate") for m in ("heat", "cold", "rain")),
            *(F.count(f"{m}_rate").alias(f"{m}_stations") for m in ("heat", "cold", "rain")),
        )
    )


def state_summary(annual):
    """Equal-weight yearly means prevent years with many reporting stations dominating."""
    return annual.groupBy("state").agg(
        *(F.avg(f"{m}_rate").alias(f"{m}_rate") for m in ("heat", "cold", "rain")),
        *(F.count(f"{m}_rate").alias(f"{m}_years") for m in ("heat", "cold", "rain")),
        F.countDistinct("year").alias("years_with_any_station"),
    )


def state_seasonal_metrics(daily, cfg):
    """DJF is calendar-year seasonal climatology (not a contiguous winter-year)."""
    station_season = _station_group_metrics(
        daily, cfg, ["year", "season"], cfg["min_obs_per_season"]
    )
    # Each station-year-season contributes equally, subject to a minimum observation count.
    return (
        station_season.groupBy("state", "season")
        .agg(
            *(F.avg(f"{m}_rate").alias(f"{m}_rate") for m in ("heat", "cold", "rain")),
            *(F.count(f"{m}_rate").alias(f"{m}_station_seasons") for m in ("heat", "cold", "rain")),
        )
    )
