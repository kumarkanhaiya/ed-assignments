"""Enterprise GenAI Adoption mini-assignment: Tasks 1-10 (PySpark)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.window import Window

CSV_PATH = Path(__file__).parent / "Enterprise_GenAI_Adoption_Impact.csv"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("GenAIAdoption")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_data(spark: SparkSession, path: Path = CSV_PATH) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )


# ---------- Task 1 ----------
def task1_overview(df: DataFrame) -> dict[str, Any]:
    """Row/column counts, unique GenAI tools, distinct industry count."""
    tools = [r[0] for r in df.select("GenAI Tool").distinct().collect()]
    return {
        "rows": df.count(),
        "columns": len(df.columns),
        "unique_genai_tools": sorted(tools),
        "distinct_industries": df.select("Industry").distinct().count(),
    }


# ---------- Task 2 ----------
def task2_standardise_columns(df: DataFrame) -> DataFrame:
    """Lowercase and replace spaces (and () and %) with underscores."""

    def clean(name: str) -> str:
        cleaned = (
            name.strip()
            .lower()
            .replace("(%)", "pct")
            .replace("(", "")
            .replace(")", "")
            .replace(" ", "_")
        )
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")
        return cleaned.strip("_")

    for old in df.columns:
        df = df.withColumnRenamed(old, clean(old))
    return df


# ---------- Task 3 ----------
def task3_null_counts(df: DataFrame) -> dict[str, int]:
    """Column -> count of null/missing values."""
    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    row = df.agg(*exprs).collect()[0].asDict()
    return {c: int(v) for c, v in row.items()}


# ---------- Task 4 ----------
def task4_cast_types(df: DataFrame) -> DataFrame:
    """Cast numeric columns to Integer/Float types."""
    return (
        df.withColumn("adoption_year", F.col("adoption_year").cast(IntegerType()))
        .withColumn(
            "productivity_change_pct",
            F.col("productivity_change_pct").cast(FloatType()),
        )
        .withColumn(
            "training_hours_provided",
            F.col("training_hours_provided").cast(IntegerType()),
        )
        .withColumn(
            "number_of_employees_impacted",
            F.col("number_of_employees_impacted").cast(IntegerType()),
        )
    )


# ---------- Task 5 ----------
def task5_add_adoption_level(df: DataFrame) -> DataFrame:
    """High if >5000, medium if 1000-5000, low if <1000."""
    return df.withColumn(
        "adoption_level",
        F.when(F.col("number_of_employees_impacted") > 5000, F.lit("High"))
        .when(F.col("number_of_employees_impacted") >= 1000, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )


# ---------- Task 6 ----------
def task6_country_industry_summary(df: DataFrame) -> DataFrame:
    """Group by country and industry: company count, avg productivity, total new roles."""
    return (
        df.groupBy("country", "industry")
        .agg(
            F.countDistinct("company_name").alias("total_companies"),
            F.avg("productivity_change_pct").alias("avg_productivity_change"),
            F.sum("new_roles_created").alias("total_new_roles_created"),
        )
        .orderBy("country", "industry")
    )


# ---------- Task 7 ----------
def task7_preprocess_sentiment(df: DataFrame) -> DataFrame:
    """Lowercase employee_sentiment and strip punctuation."""
    return df.withColumn(
        "employee_sentiment",
        F.regexp_replace(F.lower(F.col("employee_sentiment")), r"[^\w\s]", ""),
    )


# ---------- Task 8 ----------
def task8_yearwise_summary(df: DataFrame) -> DataFrame:
    """Per year: number of companies, avg training hours, most adopted GenAI tool."""
    base = df.groupBy("adoption_year").agg(
        F.countDistinct("company_name").alias("number_of_companies"),
        F.avg("training_hours_provided").alias("avg_training_hours"),
    )

    tool_counts = df.groupBy("adoption_year", "genai_tool").agg(
        F.count("*").alias("tool_count")
    )
    ranked = tool_counts.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("adoption_year").orderBy(F.col("tool_count").desc())
        ),
    )
    top_tool = ranked.filter(F.col("rn") == 1).select(
        "adoption_year", F.col("genai_tool").alias("most_adopted_tool")
    )

    return base.join(top_tool, "adoption_year", "left").orderBy("adoption_year")


# ---------- Task 9 ----------
def task9_clean_dataset(df: DataFrame) -> DataFrame:
    """Standardise columns, cast types, handle nulls, add adoption_level, trim sentiment to 100."""
    df = task2_standardise_columns(df)
    df = task4_cast_types(df)

    fill_map: dict[str, Any] = {
        "number_of_employees_impacted": 0,
        "new_roles_created": 0,
        "training_hours_provided": 0,
        "productivity_change_pct": 0.0,
        "adoption_year": 0,
    }
    df = df.fillna(fill_map)
    df = df.fillna(
        {
            "company_name": "Unknown",
            "industry": "Unknown",
            "country": "Unknown",
            "genai_tool": "Unknown",
            "employee_sentiment": "",
        }
    )

    df = task5_add_adoption_level(df)
    df = df.withColumn(
        "employee_sentiment", F.substring(F.col("employee_sentiment"), 1, 100)
    )
    return df


# ---------- Task 10 ----------
def task10_star_schema(df: DataFrame) -> dict[str, DataFrame]:
    """Star schema: dim_company, dim_time, dim_genai_tool + fact_adoption."""
    clean = task9_clean_dataset(df)

    dim_company = (
        clean.select("company_name", "industry", "country")
        .distinct()
        .withColumn("company_id", F.monotonically_increasing_id())
    )

    dim_time = (
        clean.select("adoption_year")
        .distinct()
        .withColumn("time_id", F.monotonically_increasing_id())
    )

    dim_genai_tool = (
        clean.select("genai_tool")
        .distinct()
        .withColumn("tool_id", F.monotonically_increasing_id())
    )

    fact_adoption = (
        clean.join(dim_company, ["company_name", "industry", "country"], "left")
        .join(dim_time, ["adoption_year"], "left")
        .join(dim_genai_tool, ["genai_tool"], "left")
        .select(
            "company_id",
            "time_id",
            "tool_id",
            "number_of_employees_impacted",
            "new_roles_created",
            "training_hours_provided",
            "productivity_change_pct",
            "adoption_level",
            "employee_sentiment",
        )
    )

    return {
        "dim_company": dim_company,
        "dim_time": dim_time,
        "dim_genai_tool": dim_genai_tool,
        "fact_adoption": fact_adoption,
    }


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    raw = load_data(spark)

    print("=" * 70)
    print("Task 1 - Overview")
    overview = task1_overview(raw)
    print(f"  rows: {overview['rows']}")
    print(f"  columns: {overview['columns']}")
    print(f"  distinct industries: {overview['distinct_industries']}")
    print(
        f"  unique GenAI tools ({len(overview['unique_genai_tools'])}): "
        f"{overview['unique_genai_tools']}"
    )

    print("=" * 70)
    print("Task 2 - Standardised columns")
    std = task2_standardise_columns(raw)
    print(f"  {std.columns}")

    print("=" * 70)
    print("Task 3 - Null counts per column")
    for c, n in task3_null_counts(std).items():
        print(f"  {c}: {n}")

    print("=" * 70)
    print("Task 4 - Cast types")
    casted = task4_cast_types(std)
    casted.printSchema()

    print("=" * 70)
    print("Task 5 - Adoption level distribution")
    with_level = task5_add_adoption_level(casted)
    with_level.groupBy("adoption_level").count().orderBy("adoption_level").show()

    print("=" * 70)
    print("Task 6 - Country/Industry summary (top 10)")
    task6_country_industry_summary(with_level).show(10, truncate=False)

    print("=" * 70)
    print("Task 7 - Preprocessed employee_sentiment (sample)")
    task7_preprocess_sentiment(with_level).select("employee_sentiment").show(
        5, truncate=80
    )

    print("=" * 70)
    print("Task 8 - Year-wise summary")
    task8_yearwise_summary(with_level).show(truncate=False)

    print("=" * 70)
    print("Task 9 - Cleaned dataset (sample rows and schema)")
    cleaned = task9_clean_dataset(raw)
    cleaned.printSchema()
    cleaned.show(5, truncate=60)

    print("=" * 70)
    print("Task 10 - Star schema tables")
    schema = task10_star_schema(raw)
    for name, table in schema.items():
        print(f"\n-- {name} --")
        print(f"columns: {table.columns}")
        print(f"row count: {table.count()}")
        table.show(3, truncate=60)

    spark.stop()


if __name__ == "__main__":
    main()
