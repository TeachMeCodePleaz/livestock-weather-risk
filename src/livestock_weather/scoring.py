"""Interpretable, relative weather-exposure index (NOT measured livestock losses)."""
from pyspark.sql import functions as F, Window


def score_states(summary, cfg):
    """Compare states with enough years for ALL three indicators.

    Min-max bounds are derived from the included states. Scores are therefore
    relative to this analysis cohort, not absolute risks or farm profit.
    """
    min_years = cfg["min_years_for_ranking"]
    eligible = summary.filter(
        (F.col("heat_years") >= min_years) &
        (F.col("cold_years") >= min_years) &
        (F.col("rain_years") >= min_years)
    ).dropna(subset=["heat_rate", "cold_rate", "rain_rate"])
    window_all = Window.partitionBy(F.lit(1))
    scored = eligible
    for metric in ("heat", "cold", "rain"):
        value = F.col(f"{metric}_rate")
        low = F.min(value).over(window_all)
        high = F.max(value).over(window_all)
        scored = scored.withColumn(
            f"{metric}_score",
            F.when(high == low, F.lit(0.0))
             .otherwise((value - low) / (high - low) * F.lit(100.0)),
        )
    w = cfg["weights"]
    scored = scored.withColumn(
        "overall_score",
        F.round(sum(F.col(f"{k}_score") * F.lit(float(w[k]))
                    for k in ("heat", "cold", "rain")), 2),
    )
    return scored.withColumn("rank", F.dense_rank().over(
        Window.orderBy(F.col("overall_score").asc(), F.col("state").asc())
    )).orderBy("rank")


def compare_states(scored, states):
    codes = [s.strip().upper() for s in states]
    return scored.filter(F.col("state").isin(codes)).orderBy("rank")
