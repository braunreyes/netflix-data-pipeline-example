from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName(  # type: ignore
    "Loading Test Data to Iceberg"
).getOrCreate()


def load_data():
    df = (
        spark.read.option("inferschema", "true")
        .option("header", True)
        .csv("/home/iceberg/test_data/netflix_users_raw.csv")
    )
    df.write.saveAsTable("netflix.users_raw", format="iceberg", mode="overwrite")
    df = (
        spark.read.option("inferschema", "true")
        .option("header", True)
        .csv("/home/iceberg/test_data/netflix_titles_raw.csv")
    )
    update_at_value = datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    df = df.withColumn("updated_at", lit(update_at_value))
    df.write.saveAsTable("netflix.titles_raw", format="iceberg", mode="overwrite")


def main():
    load_data()


if __name__ == "__main__":
    main()
