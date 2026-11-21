from pyspark.sql import types as T
from livestock_weather.etl import read_observations, read_stations, clean_station_days


def test_qc_and_units(spark, tmp_path):
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from make_demo_data import create_demo
    raw_dir = create_demo(tmp_path / "raw")
    raw = read_observations(spark, str(raw_dir / "*.csv"))
    stations = read_stations(spark, raw_dir / "ghcnd-stations.txt")
    assert stations.count() == 6
    clean = clean_station_days(raw, stations)
    assert clean.select("state").distinct().count() == 3
    a = clean.filter("station_id='USW00000001' and date='2000-07-01'").first()
    assert a["tmax_c"] is None  # Q_FLAG X excludes record, not replaced by zero
    b = clean.filter("station_id='USW00000002' and date='2000-04-05'").first()
    assert b["prcp_mm"] is None  # -9999 excluded, not replaced by zero
    assert clean.filter("prcp_mm = 61").count() > 0
