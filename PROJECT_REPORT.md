# Weather Data Analysis Using Apache Spark and PySpark

**Project Report — TYBSc IT (Big Data Framework)**

| | |
|---|---|
| Student name | ______________________ |
| Roll number | ______________________ |
| Guide / Incharge | ______________________ |
| College | ______________________ |
| Academic year | 2026–27 |

---

## Certificate

This is to certify that the project titled **"Weather Data Analysis Using Apache Spark and PySpark"** has been completed by the above-named student as part of the TYBSc IT curriculum.

Guide signature: ____________  Examiner signature: ____________  Date: ________

## Acknowledgement

I thank my project guide, the faculty of the IT department and my family and friends for their support during this project.

---

## Abstract

This project analyses weather data (temperature, rainfall, humidity and wind speed) using the big data framework Apache Spark through its Python API, PySpark. Daily records for five cities are loaded from a CSV file, cleaned, aggregated and summarised by Spark. The results are visualised with Matplotlib and presented in an interactive Streamlit dashboard where users can upload their own dataset and filter by city and date range.

## Table of Contents

1. Introduction
2. Objectives
3. Scope and limitations
4. Literature / technology overview
5. System requirements
6. System design
7. Dataset description
8. Implementation
9. Testing
10. Sample outputs
11. Conclusion
12. Future enhancements
13. References

---

## 1. Introduction

Weather data is collected continuously by stations and satellites, producing large volumes of records. Studying this data helps identify seasonal patterns, rainfall trends and extreme conditions, which are useful in agriculture, transport, disaster planning and daily life.

Traditional single-machine tools slow down as data grows. Apache Spark is a distributed, in-memory processing engine that can process large datasets in parallel across many cores or machines. This project uses Spark (through PySpark) to process weather records and a Streamlit web dashboard to present the results.

## 2. Objectives

1. To collect and analyse weather datasets.
2. To calculate average, maximum and minimum temperatures.
3. To analyse rainfall patterns.
4. To study changes in humidity and wind speed.
5. To compare weather conditions across different months and cities.
6. To display the results through charts and graphs.
7. To demonstrate how a big data framework processes weather information.

## 3. Scope and limitations

**In scope:** loading CSV data, cleaning, descriptive statistics, monthly and yearly aggregation, visualisation, interactive filtering.

**Limitations:**
* The project performs *analysis of past data*, not *forecasting*.
* The bundled dataset is synthetic and only illustrative; conclusions drawn from it are not real climate facts.
* Spark runs in local mode on one machine; a real cluster was not used.

## 4. Technology overview

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| Apache Spark | Distributed, in-memory data processing engine |
| PySpark | Python API for Spark (DataFrames, SQL functions) |
| Pandas / NumPy | Small result sets and dataset generation |
| Matplotlib | Charts and graphs |
| Streamlit | Web dashboard |
| CSV | Storage format for weather records |

**Why Spark?** It processes data in memory, parallelises work across CPU cores or cluster nodes, offers a high-level DataFrame API and scales from a laptop to a cluster without changing the code.

## 5. System requirements

**Hardware:** 4 GB RAM minimum (8 GB recommended), dual-core processor or better.
**Software:** Python 3.9+, Java 8/11/17, packages listed in `requirements.txt`, any modern web browser.

## 6. System design

### 6.1 Architecture

```
┌────────────────┐   ┌──────────────────────┐   ┌───────────────┐   ┌───────────────┐
│ Weather CSV    │ → │ PySpark processing   │ → │ Aggregated    │ → │ Streamlit     │
│ (upload/sample)│   │ load·clean·aggregate │   │ results (pandas)│  │ dashboard +   │
└────────────────┘   └──────────────────────┘   └───────────────┘   │ Matplotlib    │
                                                                    └───────────────┘
```

### 6.2 Modules

1. **Dataset upload** – read CSV, preview, count missing values.
2. **Temperature analysis** – average, maximum, minimum, monthly change, hottest/coldest days.
3. **Rainfall analysis** – total rainfall, wettest months, comparison across months.
4. **Humidity and wind analysis** – monthly averages and comparison.
5. **Visualisation** – line charts, bar charts, trend charts, summary table.
6. **Dashboard** – KPI tiles, city and date filters, CSV download.

### 6.3 Data flow

1. User uploads a CSV (or the sample file is used).
2. `load_data()` reads it into a Spark DataFrame and checks the required columns.
3. `clean_data()` removes duplicates and invalid values and fills missing numbers with the city average.
4. `filter_data()` applies the chosen cities and date range.
5. Aggregation functions (`overall_stats`, `monthly_summary`, `yearly_monthly_series`) run on Spark.
6. The small aggregated results are converted to pandas and plotted.

## 7. Dataset description

File: `data/weather_data.csv` — **synthetic**, 7,305 rows, 5 cities (Mumbai, Delhi, Chennai, Bengaluru, Kolkata), daily records from 2022-01-01 to 2025-12-31.

| Column | Meaning | Unit |
|---|---|---|
| Date | Observation date | YYYY-MM-DD |
| City | City name | text |
| Temperature | Daily average temperature | °C |
| Rainfall | Daily rainfall | mm |
| Humidity | Relative humidity | % |
| Wind Speed | Average wind speed | km/h |

About 0.5 % of the temperature, humidity and wind values were deliberately blanked so that the cleaning step can be demonstrated. The values are generated from rough seasonal averages with random variation; they are **not** real observations.

## 8. Implementation

### 8.1 Files

| File | Role |
|---|---|
| `src/spark_analysis.py` | All Spark logic (load, clean, filter, aggregate) |
| `src/generate_dataset.py` | Creates the sample dataset |
| `run_analysis.py` | Command-line pipeline, saves charts |
| `app.py` | Streamlit dashboard |

### 8.2 Key Spark operations used

* `spark.read.csv(..., header=True, inferSchema=True)` – load data
* `groupBy().agg()` with `avg`, `max`, `min`, `sum`, `count`, `countDistinct`
* `filter`, `dropna`, `dropDuplicates`, `coalesce`, `join`
* `year()`, `month()`, `date_format()` – date functions
* `orderBy`, `limit` – ranking

### 8.3 Cleaning logic

1. Drop rows with missing date or city.
2. Drop duplicate (date, city) rows.
3. Remove impossible values: humidity outside 0–100, negative rainfall or wind speed.
4. Fill remaining missing numeric values with the city's average.

### 8.4 How to run

See `README.md`. Dashboard: `streamlit run app.py`. Command line: `python run_analysis.py`.

## 9. Testing

| # | Test case | Expected result |
|---|---|---|
| 1 | Load the sample CSV | 7,305 rows loaded, 6 columns |
| 2 | Upload a CSV missing the `City` column | Clear error message is shown |
| 3 | Missing-value check | Non-zero counts for temperature, humidity, wind |
| 4 | After cleaning | No null values remain in numeric columns |
| 5 | Select one city | Only that city's records appear in tables and charts |
| 6 | Choose a narrow date range | KPIs and charts update accordingly |
| 7 | Date range with no data | Warning message is shown |
| 8 | Download monthly summary | CSV file downloads |

*(Record your own observed results and screenshots after running the project on your machine.)*

## 10. Sample outputs

Run the project and insert screenshots here:

* Figure 1 – Dashboard home with KPI tiles
* Figure 2 – Temperature by month
* Figure 3 – Rainfall by month
* Figure 4 – Humidity and wind comparison
* Figure 5 – Monthly trend over time

`python run_analysis.py` also saves the charts to the `output/` folder.

## 11. Conclusion

The project shows how Apache Spark can be used from Python to clean, aggregate and analyse weather records, and how a Streamlit dashboard can turn the results into an easy-to-use interface. The same code can handle much larger datasets by running Spark on a cluster.

## 12. Future enhancements

* Use real historical data from IMD, NASA POWER or Kaggle.
* Add forecasting with Spark MLlib (e.g. linear regression for temperature).
* Add anomaly detection for extreme weather.
* Add map-based visualisation and more cities.
* Store data in HDFS or a database and run on a Spark cluster.
* Stream live weather data with Spark Structured Streaming.

## 13. References

1. Apache Spark documentation — https://spark.apache.org/docs/latest/
2. PySpark API reference — https://spark.apache.org/docs/latest/api/python/
3. Streamlit documentation — https://docs.streamlit.io/
4. Matplotlib documentation — https://matplotlib.org/stable/
5. Pandas documentation — https://pandas.pydata.org/docs/
