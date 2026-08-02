"""Chipotle orders mini-assignment: Tasks 1-6."""

from pathlib import Path
import pandas as pd

TSV_PATH = Path(__file__).parent / "chipotle (1).tsv"

BEVERAGE_ITEMS = {
    "Canned Soda",
    "Canned Soft Drink",
    "Izze",
    "Nantucket Nectar",
    "6 Pack Soft Drink",
    "Bottled Water",
}


def load_data(path: Path = TSV_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t")
    df["item_price"] = (
        df["item_price"].str.replace("$", "", regex=False).str.strip().astype(float)
    )
    return df


def task1_total_revenue(df: pd.DataFrame) -> float:
    """Task 1: total revenue from all orders (quantity * item_price summed)."""
    return float((df["quantity"] * df["item_price"]).sum())


def task2_most_frequent_item(df: pd.DataFrame) -> str:
    """Task 2: most frequently ordered item by total quantity."""
    totals = df.groupby("item_name")["quantity"].sum()
    return str(totals.idxmax())


def task3_unique_items_count(df: pd.DataFrame) -> int:
    """Task 3: total number of unique items sold."""
    return int(df["item_name"].nunique())


def task4_top5_items_by_revenue(df: pd.DataFrame) -> pd.Series:
    """Task 4: top 5 items by total revenue."""
    revenue = (df["quantity"] * df["item_price"]).groupby(df["item_name"]).sum()
    return revenue.sort_values(ascending=False).head(5)


def task5_avg_items_per_order(df: pd.DataFrame) -> float:
    """Task 5: average number of items per order."""
    return float(df.groupby("order_id")["quantity"].sum().mean())


def task6_beverages_revenue(df: pd.DataFrame) -> pd.Series:
    """Task 6: beverages sold and their total revenue."""
    bev = df[df["item_name"].isin(BEVERAGE_ITEMS)].copy()
    bev["revenue"] = bev["quantity"] * bev["item_price"]
    return bev.groupby("item_name")["revenue"].sum().sort_values(ascending=False)


if __name__ == "__main__":
    df = load_data()
    print(f"Task 1 - Total revenue: ${task1_total_revenue(df):,.2f}")
    print(f"Task 2 - Most frequently ordered item: {task2_most_frequent_item(df)}")
    print(f"Task 3 - Unique items sold: {task3_unique_items_count(df)}")
    print("Task 4 - Top 5 items by revenue:")
    for name, rev in task4_top5_items_by_revenue(df).items():
        print(f"  {name}: ${rev:,.2f}")
    print(f"Task 5 - Avg items per order: {task5_avg_items_per_order(df):.2f}")
    print("Task 6 - Beverages revenue:")
    for name, rev in task6_beverages_revenue(df).items():
        print(f"  {name}: ${rev:,.2f}")
