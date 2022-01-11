from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, concat
from pyspark.sql.dataframe import DataFrame

from df_from_json import create_dataframe_from_json


def compute_big_hitters_for_blogs(df: DataFrame):
    """This adds a new column, Big hitters, based on the conditional expression"""
    df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()


def concatenate_authors_id(df: DataFrame):
    """Concatanate three columns, create a new column, and show the newly
    created concatenated column
    """
    (
        df.withColumn("AuthorsID", (concat(expr("First"), expr("Last"), expr("Id"))))
        .select(col("AuthorsID"))
        .show(4)
    )


def same_result_expr_and_col(df: DataFrame):
    """These statements return the same value, showing that expr is the same as
    a col method call
    """
    df.select(expr("Hits")).show(2)
    df.select(col("Hits")).show(2)
    df.select("Hits").show(2)


def sort_by_column(df: DataFrame, col_name: str):
    df.sort(col(col_name).desc()).show()
    df.sort(df.Id.desc()).show()


if __name__ == "__main__":
    file = "data/blogs.json"
    df = create_dataframe_from_json(file)
    compute_big_hitters_for_blogs(df)
    concatenate_authors_id(df)
    same_result_expr_and_col(df)
    sort_by_column(df, "Id")
