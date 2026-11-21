"""Load and validate pipeline settings."""
import json
from pathlib import Path


def load_settings(path):
    cfg = json.loads(Path(path).read_text(encoding="utf-8"))
    required = {"heat_c", "cold_c", "heavy_rain_mm", "min_obs_per_year",
                "min_obs_per_season", "min_years_for_ranking", "weights"}
    missing = required - set(cfg)
    if missing:
        raise ValueError(f"Missing settings: {sorted(missing)}")
    keys = {"heat", "cold", "rain"}
    if set(cfg["weights"]) != keys:
        raise ValueError("weights must define heat, cold, rain")
    weights = [float(cfg["weights"][k]) for k in ("heat", "cold", "rain")]
    if any(w < 0 for w in weights) or abs(sum(weights) - 1) > 1e-8:
        raise ValueError("Risk weights must be nonnegative and sum to 1")
    for key in ("min_obs_per_year", "min_obs_per_season", "min_years_for_ranking"):
        if not isinstance(cfg[key], int) or cfg[key] < 1:
            raise ValueError(f"{key} must be a positive integer")
    return cfg
