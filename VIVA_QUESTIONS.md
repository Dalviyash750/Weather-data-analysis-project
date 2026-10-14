# Viva Questions & Answers

## A. Project basics

**Q1. What is your project about?**
It analyses weather data (temperature, rainfall, humidity, wind speed) using Apache Spark and PySpark, and shows the results in a Streamlit dashboard with charts.

**Q2. Why is this a big data project?**
It uses Spark, a big data framework that processes data in parallel and in memory. The sample dataset is small, but the same code works on much larger data and on a cluster.

**Q3. Is the dataset real?**
The bundled dataset is synthetic, generated for demonstration. The project accepts any CSV in the same format, so a real dataset can be used.

**Q4. Is this weather analysis or weather forecasting?**
Analysis of historical data. Forecasting would need a prediction model such as Spark MLlib regression, listed as future work.

**Q5. What are the inputs and outputs?**
Input: CSV with Date, City, Temperature, Rainfall, Humidity, Wind Speed. Output: statistics tables, charts and a dashboard.

## B. Big data & Spark

**Q6. What is big data?**
Data too large or fast-growing for traditional single-machine tools. It is often described by volume, velocity and variety (the 3 Vs).

**Q7. What is Apache Spark?**
An open-source distributed engine for large-scale data processing, with in-memory computation and APIs for Python, Scala, Java and R.

**Q8. What is PySpark?**
The Python API for Spark.

**Q9. Spark vs Hadoop MapReduce?**
MapReduce writes intermediate results to disk; Spark keeps them in memory, so it is generally much faster for iterative work. Spark also has a simpler, higher-level API.

**Q10. What is a DataFrame?**
A distributed table of rows and named columns in Spark, with an optimiser that plans execution.

**Q11. What is an RDD?**
Resilient Distributed Dataset — Spark's low-level, immutable, distributed collection. DataFrames are built on top of RDDs.

**Q12. What is lazy evaluation?**
Transformations (filter, groupBy) only build a plan; nothing runs until an action (show, count, collect, toPandas) is called.

**Q13. Transformation vs action?**
Transformations create a new DataFrame (e.g. `filter`, `groupBy`); actions return results or trigger computation (e.g. `count`, `show`).

**Q14. What is a SparkSession?**
The entry point to Spark functionality. Created in `get_spark()` in `src/spark_analysis.py`.

**Q15. What does `master("local[*]")` mean?**
Run Spark on the local machine using all available CPU cores.

**Q16. What does `inferSchema=True` do?**
Spark scans the data to guess column types (e.g. double for temperature).

**Q17. Why convert to pandas at the end?**
Aggregated results are small, and Matplotlib and Streamlit work with pandas. The heavy processing is done in Spark before conversion.

**Q18. What is a cluster and how would you run this on one?**
A group of machines working together. The same code can be submitted with `spark-submit` to a cluster manager such as YARN or Kubernetes with the master URL changed.

**Q19. What is `cache()` used for?**
It keeps a DataFrame in memory so repeated actions do not recompute it. Used on the cleaned data in the dashboard.

## C. Implementation

**Q20. How do you handle missing values?**
First count them with `missing_values()`. Then rows without date/city are dropped and missing numbers are filled with that city's average.

**Q21. How do you remove duplicates?**
`dropDuplicates(["date", "city"])`.

**Q22. Which aggregate functions are used?**
`avg`, `max`, `min`, `sum`, `count`, `countDistinct` through `groupBy().agg()`.

**Q23. How is the monthly rainfall calculated?**
Total rainfall per city and month divided by the number of distinct years, giving the average monthly total.

**Q24. How are the filters applied?**
`filter_data()` uses `isin` for cities and comparison operators for the date range.

**Q25. What validation is done on the uploaded file?**
`load_data()` checks that the required columns exist and raises a clear error otherwise.

**Q26. Why Streamlit?**
It builds interactive web dashboards in pure Python with very little code.

**Q27. Why Matplotlib?**
It is the standard Python plotting library and works well with Streamlit and pandas.

**Q28. What is the role of each file?**
`spark_analysis.py` – Spark logic; `app.py` – dashboard; `run_analysis.py` – command-line runner; `generate_dataset.py` – sample data.

## D. Weather & analysis

**Q29. Which month is hottest/wettest?**
Check your own run's output. In the synthetic data, summer months are warmest and the monsoon months (June–September) are wettest for most cities, because the data was generated with that pattern.

**Q30. Why do rainfall and humidity move together?**
Rainy periods raise moisture in the air, so humidity usually rises during rainy months.

**Q31. What is the difference between average and total rainfall?**
Average is per day; total is the sum over a period. Monthly total is more meaningful for comparing seasons.

## E. Limitations & future work

**Q32. What are the limitations?**
Synthetic sample data, single-machine local mode, no forecasting.

**Q33. How can the project be improved?**
Real data (IMD/NASA POWER), Spark MLlib forecasting, anomaly detection, streaming data, HDFS storage, map visualisation.

**Q34. How would you add prediction?**
Build features (month, previous days' values), train `LinearRegression` or `RandomForestRegressor` in Spark MLlib, and evaluate with RMSE.

**Q35. How would you handle a 100 GB dataset?**
Store it in HDFS or cloud storage (preferably Parquet), run Spark on a cluster and avoid collecting large data to the driver.
