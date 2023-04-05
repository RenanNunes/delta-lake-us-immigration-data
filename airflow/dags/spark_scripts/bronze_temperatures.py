import pyspark
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        pyspark.sql.SparkSession.builder.appName("MyApp")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.2.2",
        )
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    # int96RebaseModeInWrite: config to allow dates before 1582

    temperatures_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(
            "s3a://landing-layer-udacity-nd//GlobalLandTemperaturesByCity.csv"
        )
    )
    print(temperatures_df.printSchema())
    # rename columns
    temperatures_df = temperatures_df.select(
        [
            F.col(x).alias(x.lower().replace(" ", "_").replace("-", "_"))
            for x in temperatures_df.columns
        ]
    )
    print(temperatures_df.printSchema())
    temperatures_df.write.format("delta").mode("overwrite").save(
        "s3a://bronze-layer-udacity-nd/world_temperature"
    )
