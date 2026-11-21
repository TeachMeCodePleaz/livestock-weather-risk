"""GHCN-Daily CSV + fixed-width station metadata -> US station-day measurements."""
from pyspark.sql import functions as F, types as T

US_STATES = (
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT "
    "NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC"
).split()

RAW_SCHEMA = T.StructType([
    T.StructField(name, T.StringType(), True)
    for name in ("station_id", "date_raw", "element", "data_value", "m_flag", "q_flag", "s_flag", "obs_time")
])


def read_stations(spark, stations_path):
    """NOAA fixed columns: ID 1-11, LAT 13-20, LON 22-30, STATE 39-40."""
    lines = spark.read.text(str(stations_path))
    return (
        lines.select(
            F.trim(F.substring("value", 1, 11)).alias("station_id"),
            F.trim(F.substring("value", 13, 8)).cast("double").alias("latitude"),
            F.trim(F.substring("value", 22, 9)).cast("double").alias("longitude"),
            F.trim(F.substring("value", 39, 2)).alias("state"),
        )
        .filter(F.col("station_id").startswith("US") & F.col("state").isin(US_STATES))
        .dropDuplicates(["station_id"])
    )


def read_observations(spark, path_or_glob):
    """NOAA by_year files have 8 comma-separated fields and NO header."""
    return spark.read.option("header", "false").schema(RAW_SCHEMA).csv(str(path_or_glob))


def clean_station_days(raw, stations):
    """Keep QC-passed TMAX/TMIN/PRCP; convert 0.1 C and 0.1 mm to SI.

    Important: absent values stay null (NOT zero). A trace PRCP value of 0
    remains 0. Duplicate station/date/element rows are resolved by max value;
    duplicates should be audited if observed in production.
    """
    obs = (
        raw.filter(F.col("element").isin("TMAX", "TMIN", "PRCP"))
        .filter(F.col("q_flag").isNull() | (F.trim(F.col("q_flag")) == ""))
        .withColumn("value_num", F.col("data_value").cast("double"))
        .filter(F.col("value_num").isNotNull() & (F.col("value_num") != -9999))
        .withColumn("date", F.to_date("date_raw", "yyyyMMdd"))
        .filter(F.col("date").isNotNull())
        .join(F.broadcast(stations), "station_id", "inner")
    )
    daily = (
        obs.groupBy("station_id", "state", "latitude", "longitude", "date")
        .agg(
            F.max(F.when(F.col("element") == "TMAX", F.col("value_num") / 10.0)).alias("tmax_c"),
            F.max(F.when(F.col("element") == "TMIN", F.col("value_num") / 10.0)).alias("tmin_c"),
            F.max(F.when(F.col("element") == "PRCP", F.col("value_num") / 10.0)).alias("prcp_mm"),
        )
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("season", F.when(F.col("month").isin(12, 1, 2), "DJF")
                    .when(F.col("month").isin(3, 4, 5), "MAM")
                    .when(F.col("month").isin(6, 7, 8), "JJA").otherwise("SON"))
    )
    return daily
