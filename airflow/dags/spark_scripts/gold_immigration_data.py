import pyspark
from pyspark.sql import functions as F


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.2.2")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    i94_immigration_df = spark.read.format("delta").load('s3a://silver-layer-udacity-nd/i94_immigration')
    print(i94_immigration_df.printSchema())
    # read city and temperature tables
    us_cities_df = spark.read.format("delta").load('s3a://silver-layer-udacity-nd/us_city_demographic')
    weather_df = spark.read.format("delta").load('s3a://silver-layer-udacity-nd/world_temperature')
    # group immigration data
    i94_immigration_grouped = i94_immigration_df.select(F.col("i94cit_value").alias("origin_country"), F.col("i94res_value").alias("resident_country"),
        F.col("application_state"), F.col("application_city"), F.col("arrdate").alias("arrival_date"),
        F.col("i94mode_value"), F.col("i94visa_value"))\
        .groupby("origin_country", "resident_country", "application_state", "application_city",
        "arrival_date", "i94mode_value", "i94visa_value").count()
    print(i94_immigration_grouped.printSchema())
    # join city data
    i94_immigration_city = i94_immigration_grouped.alias("df1").join(
        us_cities_df.alias("df2"), (F.lower(i94_immigration_grouped.application_state) == F.lower(us_cities_df.state_code)) &
        (F.lower(i94_immigration_grouped.application_city) == F.lower(us_cities_df.city)), "inner")\
        .select([F.col('df1.'+ x) for x in i94_immigration_grouped.columns] + [F.col('df2.total_population').alias("total_city_population")])\
        .dropDuplicates()
    # join weather data (it's commented because the dataset only has data until 2013)
    # gold_immigration_df = i94_immigration_city.join(weather_df.alias("df3"), (i94_immigration_grouped.arrival_date == weather_df.dt) & \
    #         (F.lower(i94_immigration_grouped.application_city) == F.lower(weather_df.city)), "inner")\
    #     .select([F.col('df1.'+ x) for x in i94_immigration_grouped.columns] + [F.col('df3.averagetemperature').alias("city_average_temperature")])\
    #     .dropDuplicates()
    # upload gold table
    # gold_immigration_df.write.format("delta").mode("overwrite").save("s3a://gold-layer-udacity-nd/grouped_immigration_data")
    i94_immigration_city.write.format("delta").mode("overwrite").save("s3a://gold-layer-udacity-nd/grouped_immigration_data")
