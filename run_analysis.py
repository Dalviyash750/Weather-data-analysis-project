"""
run_analysis.py
---------------
Command-line version of the project (no dashboard). Runs the full PySpark
pipeline and saves charts + CSV results to the output/ folder.

Usage:
    python run_analysis.py
    python run_analysis.py --input data/weather_data.csv --city Mumbai
"""

import argparse
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import spark_analysis as sa  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Weather analysis with PySpark")
    ap.add_argument("--input", default="data/weather_data.csv")
    ap.add_argument("--city", default=None, help="analyse a single city (default: all)")
    ap.add_argument("--out", default="output")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    spark = sa.get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    raw = sa.load_data(spark, args.input)
    print("\n=== Missing values (before cleaning) ===")
    sa.missing_values(raw).show()

    df = sa.clean_data(raw)
    if args.city:
        df = sa.filter_data(df, cities=[args.city])
    print(f"Rows after cleaning: {df.count():,}")

    print("\n=== Overall statistics per city ===")
    stats = sa.overall_stats(df)
    stats.show(truncate=False)

    print("=== Top 5 wettest months ===")
    sa.top_rainfall_months(df).show()
    print("=== Top 5 hottest days ===")
    sa.hottest_days(df).show()

    monthly = sa.monthly_summary(df).toPandas()
    series = sa.yearly_monthly_series(df).toPandas()
    stats.toPandas().to_csv(f"{args.out}/overall_stats.csv", index=False)
    monthly.to_csv(f"{args.out}/monthly_summary.csv", index=False)

    # Chart 1: monthly average temperature
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for city, g in monthly.groupby("city"):
        ax.plot(g["month_name"], g["avg_temp"], marker="o", label=city)
    ax.set(title="Average Temperature by Month", ylabel="°C")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{args.out}/temperature_by_month.png", dpi=150)

    # Chart 2: monthly rainfall bars
    fig, ax = plt.subplots(figsize=(9, 4.5))
    pivot = monthly.pivot(index="month", columns="city", values="avg_monthly_rain")
    pivot.plot(kind="bar", ax=ax)
    ax.set(title="Average Monthly Rainfall", ylabel="mm", xlabel="Month")
    fig.tight_layout()
    fig.savefig(f"{args.out}/rainfall_by_month.png", dpi=150)

    # Chart 3: humidity vs wind
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for city, g in monthly.groupby("city"):
        axes[0].plot(g["month_name"], g["avg_humidity"], label=city)
        axes[1].plot(g["month_name"], g["avg_wind"], label=city)
    axes[0].set(title="Average Humidity (%)")
    axes[1].set(title="Average Wind Speed (km/h)")
    axes[0].legend()
    fig.tight_layout()
    fig.savefig(f"{args.out}/humidity_wind.png", dpi=150)

    # Chart 4: full time series of temperature
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for city, g in series.groupby("city"):
        ax.plot(g["year_month"], g["avg_temp"], label=city)
    ax.set(title="Monthly Average Temperature (2022-2025)", ylabel="°C")
    ax.set_xticks(ax.get_xticks()[::6])
    plt.setp(ax.get_xticklabels(), rotation=45)
    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{args.out}/temperature_trend.png", dpi=150)

    print(f"\nDone. Charts and CSV files saved in '{args.out}/'.")
    spark.stop()


if __name__ == "__main__":
    main()
