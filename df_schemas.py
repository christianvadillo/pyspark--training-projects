"""
DataFrames are like distributed in-memory tables with named columns and schemas.
DataFrames are immutable and Spark keeps a lineage of all transformations.
The following example shows how to create a DataFrame from a list of tuples defining
a schema using Data Definition Language (DDL).
"""

from pyspark.sql import SparkSession


# fmt: off
STATIC_DATA = [
        [1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
        [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
        [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
        [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
        ]
# fmt: on


def create_dataframe_and_print():
    # Define the schema using DDL
    schema = (
        "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, "
        "`Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
    )

    # Create a SparkSession
    spark = SparkSession.builder.appName("CreateSchema").getOrCreate()

    # Create a DataFrame from the static data using the defined schema
    df = spark.createDataFrame(STATIC_DATA, schema)

    # Show the DataFrame
    df.show()

    # Print the schema used by Spark to process the DataFrame
    print("Spark Schema:")
    print(df.printSchema())


if __name__ == "__main__":
    create_dataframe_and_print()
