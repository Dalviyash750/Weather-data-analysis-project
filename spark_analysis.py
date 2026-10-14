"""
spark_analysis.py
-----------------
Core PySpark functions for the Weather Data Analysis project.

Expected CSV columns (case/space insensitive):
    Date, City, Temperature, Rainfall, Humidity, Wind Speed
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

NUMERIC_COLS = ["temperature", "rainfall", "humidity", "wind_speed"]
REQUIRED_COLS = ["date", "city"] + NUMERIC_COLS


def get_spark(app_name: str = "WeatherDataAnalysis") -> SparkSession:
    """Create (or reuse) a local SparkSession that uses all CPU cores."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_data(spark: SparkSession, path: str) -> DataFrame:
    """Read the CSV, normalise column names and validate the schema."""
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"CSV is missing required column(s): {missing}. "
            f"Found: {df.columns}"
        )
    df = df.select(*REQUIRED_COLS)
    df = df.withColumn("date", F.to_date("date"))
    for c in NUMERIC_COLS:
        df = df.withColumn(c, F.col(c).cast("double"))
    return df


def missing_values(df: DataFrame) -> DataFrame:
    """Count missing (null / NaN) values in every column."""
    exprs = []
    for c in df.columns:
        if c in NUMERIC_COLS:
            cond = F.col(c).isNull() | F.isnan(F.col(c))
        else:
            cond = F.col(c).isNull()
        exprs.append(F.sum(cond.cast("int")).alias(c))
    return df.agg(*exprs)


def clean_data(df: DataFrame) -> DataFrame:
    """
    Cleaning steps:
      1. drop rows without a date or city
      2. drop duplicate (date, city) rows
      3. remove impossible values (humidity outside 0-100, negative rainfall/wind)
      4. fill remaining missing numeric values with the city's average
    """
    df = df.dropna(subset=["date", "city"]).dropDuplicates(["date", "city"])
    df = df.filter(
        (F.col("humidity").isNull() | F.col("humidity").between(0, 100))
        & (F.col("rainfall").isNull() | (F.col("rainfall") >= 0))
        & (F.col("wind_speed").isNull() | (F.col("wind_speed") >= 0))
    )
    means = df.groupBy("city").agg(*[F.avg(c).alias(f"avg_{c}") for c in NUMERIC_COLS])
    df = df.join(means, on="city", how="left")
    for c in NUMERIC_COLS:
        df = df.withColumn(c, F.coalesce(F.col(c), F.col(f"avg_{c}")))
    return df.select(*REQUIRED_COLS)


def add_time_columns(df: DataFrame) -> DataFrame:
    """Add year, month number and month name columns."""
    return (
        df.withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMM"))
    )


def filter_data(df: DataFrame, cities=None, start=None, end=None) -> DataFrame:
    """Filter by list of cities and/or an inclusive date range."""
    if cities:
        df = df.filter(F.col("city").isin(list(cities)))
    if start is not None:
        df = df.filter(F.col("date") >= F.lit(start))
    if end is not None:
        df = df.filter(F.col("date") <= F.lit(end))
    return df


def overall_stats(df: DataFrame) -> DataFrame:
    """Summary statistics (average / max / min) per city."""
    return (
        df.groupBy("city")
        .agg(
            F.count("*").alias("records"),
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.max("temperature").alias("max_temp"),
            F.min("temperature").alias("min_temp"),
            F.round(F.sum("rainfall"), 1).alias("total_rain"),
            F.round(F.avg("humidity"), 2).alias("avg_humidity"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
        )
        .orderBy("city")
    )


def monthly_summary(df: DataFrame) -> DataFrame:
    """Month-by-month aggregates for each city (all years combined)."""
    df = add_time_columns(df)
    return (
        df.groupBy("city", "month", "month_name")
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.max("temperature").alias("max_temp"),
            F.min("temperature").alias("min_temp"),
            # total rainfall averaged over the number of years in the data
            F.round(F.sum("rainfall") / F.countDistinct("year"), 1).alias("avg_monthly_rain"),
            F.round(F.avg("humidity"), 2).alias("avg_humidity"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
        )
        .orderBy("city", "month")
    )


def yearly_monthly_series(df: DataFrame) -> DataFrame:
    """Year-month time series (used for the trend line chart)."""
    df = add_time_columns(df)
    return (
        df.withColumn("year_month", F.date_format("date", "yyyy-MM"))
        .groupBy("city", "year_month")
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.round(F.sum("rainfall"), 1).alias("total_rain"),
            F.round(F.avg("humidity"), 2).alias("avg_humidity"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
        )
        .orderBy("city", "year_month")
    )


def top_rainfall_months(df: DataFrame, n: int = 5) -> DataFrame:
    """The n wettest year-months across the selection."""
    return yearly_monthly_series(df).orderBy(F.desc("total_rain")).limit(n)


def hottest_days(df: DataFrame, n: int = 5) -> DataFrame:
    return df.orderBy(F.desc("temperature")).select("date", "city", "temperature").limit(n)


def coldest_days(df: DataFrame, n: int = 5) -> DataFrame:
    return df.orderBy(F.asc("temperature")).select("date", "city", "temperature").limit(n)
