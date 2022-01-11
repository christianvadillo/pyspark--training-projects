"""
Read data from a JSON file into a Spark DataFrame using a defined schema.
# Source: https://github.com/databricks/LearningSparkV2/tree/master/chapter3

"""
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from pyspark.sql.types import (
    IntegerType,
    StringType,
    ArrayType,
    StructField,
    StructType,
)


def create_dataframe_from_json(file: str) -> DataFrame:
    # 1. Create a SparkSession
    spark = SparkSession.builder.appName("ReadJson").getOrCreate()

    # 2. Define the schema programmatically
    schema = StructType(
        [
            StructField("Id", IntegerType(), False),
            StructField("First", StringType(), False),
            StructField("Last", StringType(), False),
            StructField("Url", StringType(), False),
            StructField("Published", StringType(), False),
            StructField("Hits", IntegerType(), False),
            StructField("Campaigns", ArrayType(StringType(), False)),
        ]
    )

    # 3. Read the JSON file into a Spark DataFrame
    df = spark.read.schema(schema).json(file)

    # 4. Show the DataFrame
    df.show()

    # 5. Print the schema used by Spark to process the DataFrame
    print(df.printSchema())

    return df


if __name__ == "__main__":
    file = "data/blogs.json"
    create_dataframe_from_json(file)
