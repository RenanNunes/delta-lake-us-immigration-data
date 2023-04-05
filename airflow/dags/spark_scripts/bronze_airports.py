import pyspark

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
        .getOrCreate()
    )

    airport_codes_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("s3a://landing-layer-udacity-nd//airport-codes_csv.csv")
    )
    print(airport_codes_df.printSchema())
    airport_codes_df.write.format("delta").mode("overwrite").save(
        "s3a://bronze-layer-udacity-nd/airport_codes"
    )
