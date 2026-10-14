"""
generate_dataset.py
-------------------
Creates a SYNTHETIC weather dataset (daily records, 5 Indian cities,
2022-2025) so the project can run without downloading anything.

The monthly averages below are rough, illustrative climate values used only
to make realistic-looking data. They are NOT verified historical records.
Replace data/weather_data.csv with a real dataset (e.g. from Kaggle, IMD or
NASA POWER) in the same column format to analyse real weather.

Usage:
    python src/generate_dataset.py
"""

import os

import numpy as np
import pandas as pd

rng = np.random.default_rng(42)

# month-wise illustrative climate: avg temp (C), rainfall (mm/month), humidity (%)
CLIMATE = {
    "Mumbai": dict(
        temp=[24.5, 25.5, 27.5, 29.5, 30.5, 29.5, 28, 27.5, 28, 29, 28, 26],
        rain=[0.5, 0.5, 0, 2, 12, 520, 840, 550, 350, 90, 15, 3],
        hum=[62, 63, 66, 68, 70, 82, 87, 85, 80, 72, 63, 61],
        wind=11,
    ),
    "Delhi": dict(
        temp=[14, 17, 23, 29, 33, 34, 31, 30, 29, 25, 19, 15],
        rain=[20, 18, 15, 10, 25, 75, 210, 230, 125, 15, 5, 10],
        hum=[70, 62, 50, 38, 38, 52, 73, 77, 66, 56, 58, 69],
        wind=9,
    ),
    "Chennai": dict(
        temp=[25, 26.5, 28.5, 31, 33, 32.5, 31, 30.5, 30.5, 28.5, 26.5, 25],
        rain=[25, 8, 10, 15, 50, 50, 90, 115, 120, 270, 350, 140],
        hum=[70, 70, 68, 68, 62, 60, 63, 65, 67, 76, 80, 74],
        wind=13,
    ),
    "Bengaluru": dict(
        temp=[22, 24.5, 27, 28.5, 27.5, 25, 24, 24, 24.5, 24, 22.5, 21.5],
        rain=[3, 8, 12, 45, 110, 80, 110, 140, 180, 160, 60, 15],
        hum=[66, 58, 52, 56, 62, 70, 74, 75, 74, 74, 71, 69],
        wind=10,
    ),
    "Kolkata": dict(
        temp=[20, 23.5, 28, 30.5, 30.5, 30, 29, 29, 29, 27.5, 24, 20.5],
        rain=[10, 25, 30, 50, 130, 300, 330, 330, 290, 140, 15, 5],
        hum=[75, 70, 67, 72, 75, 82, 86, 86, 85, 80, 73, 74],
        wind=8,
    ),
}


def build() -> pd.DataFrame:
    dates = pd.date_range("2022-01-01", "2025-12-31", freq="D")
    frames = []
    for city, c in CLIMATE.items():
        m = dates.month.values - 1
        n = len(dates)
        temp = np.array(c["temp"])[m] + rng.normal(0, 1.6, n)

        monthly_rain = np.array(c["rain"], dtype=float)[m]
        p_rain = np.clip(0.03 + monthly_rain / 600, 0.02, 0.85)
        mean_per_rainy_day = monthly_rain / (p_rain * 30.4)
        rainy = rng.random(n) < p_rain
        rain = np.where(rainy, rng.exponential(np.maximum(mean_per_rainy_day, 0.1)), 0.0)

        hum = np.array(c["hum"])[m] + rng.normal(0, 4, n) + np.where(rainy, 5, 0)
        hum = np.clip(hum, 20, 100)

        monsoon = ((m >= 5) & (m <= 8)).astype(float)
        wind = np.clip(c["wind"] + 3 * monsoon + rng.normal(0, 2.5, n), 1, None)

        frames.append(
            pd.DataFrame(
                {
                    "Date": dates.strftime("%Y-%m-%d"),
                    "City": city,
                    "Temperature": temp.round(1),
                    "Rainfall": rain.round(1),
                    "Humidity": hum.round(0),
                    "Wind Speed": wind.round(1),
                }
            )
        )
    df = pd.concat(frames, ignore_index=True)

    # inject ~0.5% missing values so the cleaning step has something to do
    for col in ["Temperature", "Humidity", "Wind Speed"]:
        idx = rng.choice(df.index, size=int(len(df) * 0.005), replace=False)
        df.loc[idx, col] = np.nan
    return df


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(__file__), "..", "data", "weather_data.csv")
    out = os.path.abspath(out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    data = build()
    data.to_csv(out, index=False)
    print(f"Wrote {len(data):,} rows to {out}")
