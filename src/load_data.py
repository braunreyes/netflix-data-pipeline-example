from pyspark.sql import SparkSession

spark = SparkSession.builder.appName( # type: ignore
    "Loading Test Data to Iceberg"
).getOrCreate()

def load_data():
    df = spark.read.option("inferschema", "true").csv(
        "/home/iceberg/test_data/netflix_users_raw.csv"
    )
    df.write.saveAsTable("netflix.users_raw", format="iceberg", mode="overwrite")
    df = spark.read.option("inferschema", "true").csv(
        "/home/iceberg/test_data/netflix_titles_raw.csv"
    )
    df.write.saveAsTable("netflix.titles_raw", format="iceberg", mode="overwrite")


def main():
    load_data()


if __name__ == "__main__":
    main()
