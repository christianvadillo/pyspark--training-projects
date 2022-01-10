"""
Spark program that reads a file with over 100,000 entries (where each row
or line hast a <state, mm_color, count>) and computes and aggregates the
counts for each color and state.

These aggregated counts tell us the colors of M&Ms favored by students in each
state

Source: https://github.com/databricks/LearningSparkV2
"""

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count


def get_session() -> SparkSession:
    """
    Get a SparkSession
    """
    return SparkSession.builder.appName("MnMCount").getOrCreate()


def read_or_load_data(spark: SparkSession, file_path: str) -> DataFrame:
    """Load the data from the file and return a Spark's DataFrame object
    # * Format by inferring the schema
    # * Specify that the file contains a header
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path)
    )


def count_by_states(spark: SparkSession, df: DataFrame) -> None:
    # -------------------------------------------------------------------------
    # * Because some Spark's functions return same object, we can chain functions
    # * 1. Select from the DataFrame the fields "State", "Color", and "Count"
    # * 2. Group by the fields "State" and "Color"
    # * 3. Aggregate the "Count" field by the "count" function
    # * 4. Sort the results by "State" and "Color" in descending order

    count_df = (
        df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )
    # -------------------------------------------------------------------------
    # Show the results
    count_df.show(n=60, truncate=False)
    print("Total number of rows:", count_df.count())


def count_by_specific_state(spark: SparkSession, df: DataFrame, state: str) -> None:
    count_df = (
        df.select("State", "Color", "Count")
        .filter(df.State == state)
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )
    count_df.show(n=10, truncate=False)

    # Alternative way to do the same thing
    count_df = (
        df.select("State", "Color", "Count")
        .where(df.State == state)
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    count_df.show(n=10, truncate=False)


def main():
    file_dir = "data/mnm_dataset.csv"
    # 1. Build a SparkSession
    spark = get_session()
    # 2. Read the dataset into a Spark DataFrame
    mnm_df = read_or_load_data(spark, file_dir)
    # 3. Compute the counts for each color and state
    count_by_states(spark, mnm_df)
    # 3.1 Compute the counts for a specific state
    count_by_specific_state(spark, mnm_df, state="TX")
    # 4. Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
