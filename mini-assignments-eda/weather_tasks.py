"""Weather dataset mini-assignment: Tasks 1-5."""

from pathlib import Path
import pandas as pd

CSV_PATH = Path(__file__).parent / "Weather Dataset - CSV(in).csv"


def load_data(path: Path = CSV_PATH) -> pd.DataFrame:
    return pd.read_csv(path)


def task1_rainy_next_day_count(df: pd.DataFrame) -> int:
    """Task 1: number of days when it rained the next day."""
    return int((df["RainTomorrow"] == "Yes").sum())


def task2_avg_sunshine_no_rain(df: pd.DataFrame) -> float:
    """Task 2: average sunshine duration on days with no rainfall."""
    return float(df.loc[df["Rainfall"] == 0, "Sunshine"].mean())


def task3_max_temp_3pm(df: pd.DataFrame) -> float:
    """Task 3: maximum temperature recorded at 3 PM."""
    return float(df["Temp3pm"].max())


def task4_avg_humidity_3pm_rain_tomorrow(df: pd.DataFrame) -> float:
    """Task 4: average humidity at 3 PM on days it rained the next day."""
    return float(df.loc[df["RainTomorrow"] == "Yes", "Humidity3pm"].mean())


def task5_common_wind_dir_9am_cloudy(df: pd.DataFrame, threshold: int = 5) -> str:
    """Task 5: most common wind direction at 9 AM on cloudy days (Cloud9am > threshold)."""
    cloudy = df.loc[df["Cloud9am"] > threshold, "WindDir9am"].dropna()
    return str(cloudy.mode().iloc[0])


if __name__ == "__main__":
    df = load_data()
    print(f"Task 1 - Days it rained the next day: {task1_rainy_next_day_count(df)}")
    print(f"Task 2 - Avg sunshine on no-rain days: {task2_avg_sunshine_no_rain(df):.2f}")
    print(f"Task 3 - Max Temp at 3 PM: {task3_max_temp_3pm(df)}")
    print(f"Task 4 - Avg Humidity3pm when RainTomorrow=Yes: {task4_avg_humidity_3pm_rain_tomorrow(df):.2f}")
    print(f"Task 5 - Most common WindDir9am on cloudy days (Cloud9am>5): {task5_common_wind_dir_9am_cloudy(df)}")
