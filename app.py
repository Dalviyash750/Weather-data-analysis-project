"""
app.py - Streamlit dashboard for Weather Data Analysis using PySpark.

Run:
    streamlit run app.py
"""

import os
import sys
import tempfile

import matplotlib.pyplot as plt
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import spark_analysis as sa  # noqa: E402

SAMPLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "weather_data.csv")

st.set_page_config(page_title="Weather Data Analysis (PySpark)", page_icon="🌦️", layout="wide")


@st.cache_resource
def spark_session():
    s = sa.get_spark()
    s.sparkContext.setLogLevel("ERROR")
    return s


spark = spark_session()

st.title("🌦️ Weather Data Analysis using Apache Spark & PySpark")
st.caption("Big Data Framework project — TYBSc IT")

# ---------------- Module 1: dataset upload ----------------
st.sidebar.header("1. Dataset")
uploaded = st.sidebar.file_uploader("Upload weather CSV", type=["csv"])
if uploaded is not None:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.write(uploaded.getvalue())
    tmp.close()
    path = tmp.name
    st.sidebar.success(f"Using uploaded file: {uploaded.name}")
else:
    path = SAMPLE
    st.sidebar.info("Using bundled sample dataset (synthetic).")

try:
    raw = sa.load_data(spark, path)
except Exception as e:  # noqa: BLE001
    st.error(f"Could not read the dataset: {e}")
    st.stop()

with st.expander("📂 Module 1 — Dataset preview & missing values", expanded=False):
    st.write("**First 20 rows**")
    st.dataframe(raw.limit(20).toPandas(), use_container_width=True)
    st.write("**Missing values per column (before cleaning)**")
    st.dataframe(sa.missing_values(raw).toPandas(), use_container_width=True)

df = sa.clean_data(raw).cache()

# ---------------- Module 6: filters ----------------
st.sidebar.header("2. Filters")
all_cities = [r["city"] for r in df.select("city").distinct().orderBy("city").collect()]
cities = st.sidebar.multiselect("City", all_cities, default=all_cities[:2] or all_cities)
bounds = df.agg({"date": "min"}).collect()[0][0], df.agg({"date": "max"}).collect()[0][0]
date_range = st.sidebar.date_input("Date range", value=bounds, min_value=bounds[0], max_value=bounds[1])
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start, end = date_range
else:
    start, end = bounds

fdf = sa.filter_data(df, cities=cities, start=start, end=end).cache()
n = fdf.count()
if n == 0:
    st.warning("No records for the selected filters.")
    st.stop()

# KPIs
stats = sa.overall_stats(fdf).toPandas()
monthly = sa.monthly_summary(fdf).toPandas()
series = sa.yearly_monthly_series(fdf).toPandas()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Records analysed", f"{n:,}")
k2.metric("Avg temperature", f"{fdf.agg({'temperature': 'avg'}).collect()[0][0]:.1f} °C")
k3.metric("Max temperature", f"{fdf.agg({'temperature': 'max'}).collect()[0][0]:.1f} °C")
k4.metric("Total rainfall", f"{fdf.agg({'rainfall': 'sum'}).collect()[0][0]:,.0f} mm")

st.subheader("Summary statistics per city")
st.dataframe(stats, use_container_width=True)

tab_t, tab_r, tab_h, tab_tr = st.tabs(
    ["🌡️ Temperature", "🌧️ Rainfall", "💧 Humidity & Wind", "📈 Trend over time"]
)

with tab_t:  # Module 2
    fig, ax = plt.subplots(figsize=(9, 4))
    for city, g in monthly.groupby("city"):
        ax.plot(g["month_name"], g["avg_temp"], marker="o", label=city)
    ax.set(title="Average temperature by month", ylabel="°C")
    ax.grid(alpha=0.3)
    ax.legend()
    st.pyplot(fig)
    hot = monthly.loc[monthly["avg_temp"].idxmax()]
    cold = monthly.loc[monthly["avg_temp"].idxmin()]
    st.info(
        f"Hottest month on average: **{hot['month_name']}** ({hot['city']}, {hot['avg_temp']} °C). "
        f"Coolest: **{cold['month_name']}** ({cold['city']}, {cold['avg_temp']} °C)."
    )
    c1, c2 = st.columns(2)
    c1.write("**Hottest days**")
    c1.dataframe(sa.hottest_days(fdf).toPandas(), use_container_width=True)
    c2.write("**Coldest days**")
    c2.dataframe(sa.coldest_days(fdf).toPandas(), use_container_width=True)

with tab_r:  # Module 3
    fig, ax = plt.subplots(figsize=(9, 4))
    monthly.pivot(index="month", columns="city", values="avg_monthly_rain").plot(kind="bar", ax=ax)
    ax.set(title="Average monthly rainfall", ylabel="mm", xlabel="Month")
    st.pyplot(fig)
    wet = monthly.loc[monthly["avg_monthly_rain"].idxmax()]
    st.info(f"Wettest month on average: **{wet['month_name']}** ({wet['city']}, {wet['avg_monthly_rain']} mm).")
    st.write("**Top 5 wettest year-months**")
    st.dataframe(sa.top_rainfall_months(fdf).toPandas(), use_container_width=True)

with tab_h:  # Module 4
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for city, g in monthly.groupby("city"):
        axes[0].plot(g["month_name"], g["avg_humidity"], marker="o", label=city)
        axes[1].plot(g["month_name"], g["avg_wind"], marker="o", label=city)
    axes[0].set(title="Average humidity (%)")
    axes[1].set(title="Average wind speed (km/h)")
    axes[0].legend()
    st.pyplot(fig)

with tab_tr:  # Module 5
    metric = st.selectbox(
        "Metric", ["avg_temp", "total_rain", "avg_humidity", "avg_wind"], index=0
    )
    fig, ax = plt.subplots(figsize=(10, 4))
    for city, g in series.groupby("city"):
        ax.plot(g["year_month"], g[metric], label=city)
    ax.set_xticks(ax.get_xticks()[:: max(1, len(ax.get_xticks()) // 12)])
    plt.setp(ax.get_xticklabels(), rotation=45)
    ax.set(title=f"{metric} over time")
    ax.legend()
    st.pyplot(fig)
    st.download_button(
        "⬇️ Download monthly summary (CSV)",
        monthly.to_csv(index=False),
        file_name="monthly_summary.csv",
    )

st.caption("Data processing: Apache Spark (PySpark) • Dashboard: Streamlit • Charts: Matplotlib")
