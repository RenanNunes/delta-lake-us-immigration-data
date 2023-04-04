import pyspark
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.2.2")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    us_cities_df = spark.read.option("header", True)\
                        .option("delimiter", ";")\
                        .option("inferSchema", True)\
                        .csv("s3a://landing-layer-udacity-nd/us-cities-demographics.csv")
    print(us_cities_df.printSchema())
    # rename columns
    us_cities_df = us_cities_df.select([F.col(x).alias(x.lower().replace(' ', '_').replace('-', '_')) \
                        for x in us_cities_df.columns])
    print(us_cities_df.printSchema())
    us_cities_df.write.format("delta").mode("overwrite").save("s3a://bronze-layer-udacity-nd/us_city_demographic")
