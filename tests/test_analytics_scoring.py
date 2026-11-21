from livestock_weather.analytics import state_year_metrics, state_summary, state_seasonal_metrics
from livestock_weather.scoring import score_states, compare_states
from livestock_weather.settings import load_settings


def test_rate_denominators_ignore_missing(spark):
    # Same station, 3 days: one missing heat observation => heat denominator = 2.
    schema = "station_id string,state string,year int,season string,tmax_c double,tmin_c double,prcp_mm double"
    daily = spark.createDataFrame([
        ("S1", "TX", 2000, "JJA", 40.0, -2.0, 0.0),
        ("S1", "TX", 2000, "JJA", None, -2.0, 60.0),
        ("S1", "TX", 2000, "JJA", 20.0, -2.0, 0.0),
    ], schema)
    cfg = load_settings("config/demo.json")
    cfg["min_obs_per_year"] = 1
    year = state_year_metrics(daily, cfg).first()
    assert year["heat_rate"] == 0.5
    assert abs(year["rain_rate"] - 1 / 3) < 1e-8
    season = state_seasonal_metrics(daily, cfg).first()
    assert season["heat_rate"] == 0.5


def test_scoring_relative_ranking_and_comparison(spark):
    cfg = load_settings("config/demo.json")
    rows = [
        ("TX", 0.9, 0.0, 0.5, 2, 2, 2),
        ("IA", 0.4, 0.4, 0.3, 2, 2, 2),
        ("WI", 0.0, 0.8, 0.1, 2, 2, 2),
    ]
    schema = "state string,heat_rate double,cold_rate double,rain_rate double,heat_years int,cold_years int,rain_years int"
    scores = score_states(spark.createDataFrame(rows, schema), cfg)
    got = {r["state"]: r["overall_score"] for r in scores.collect()}
    assert len(got) == 3
    assert abs(got["TX"] - 65.0) < 1e-5
    assert abs(got["WI"] - 35.0) < 1e-5
    assert compare_states(scores, ["wi", "tx"]).count() == 2
