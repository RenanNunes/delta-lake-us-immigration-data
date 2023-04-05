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

    us_city_demographic_df = spark.read.format("delta").load(
        "s3a://bronze-layer-udacity-nd/us_city_demographic"
    )
    print(us_city_demographic_df.printSchema())
    # no need for formatting on this table
    us_city_demographic_df.write.format("delta").mode("overwrite").save(
        "s3a://silver-layer-udacity-nd/us_city_demographic"
    )
