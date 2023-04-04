import pyspark
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.2.2")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    temperatures_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/world_temperature')
    print(temperatures_df.printSchema())
    # filter out before 2010 because it won't be needed
    temperatures_filtered_df = temperatures_df.filter(F.col("dt") > F.lit("2010-01-01"))
    temperatures_filtered_df.write.format("delta").mode("overwrite").save("s3a://silver-layer-udacity-nd/world_temperature")
