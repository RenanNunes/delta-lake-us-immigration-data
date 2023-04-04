import pyspark

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.2.2")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    i94_immigration_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94_immigration')
    print(i94_immigration_df.printSchema())
    # read i94 tables
    i94cit_res_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94cit_res')
    i94port_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94port')
    i94mode_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94mode')
    i94addr_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94addr')
    i94visa_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94visa')
    # join i94 code tables
    silver_i94_immigration_df = i94_immigration_df.join(i94cit_res_df, i94_immigration_df.i94cit == i94cit_res_df.code, "inner")\
        .drop("code").withColumnRenamed("value", "i94cit_value")\
        .join(i94cit_res_df, i94_immigration_df.i94res == i94cit_res_df.code, "inner")\
        .drop("code").withColumnRenamed("value", "i94res_value")\
        .join(i94port_df, i94_immigration_df.i94port == i94port_df.code, "inner")\
        .drop("code").withColumnRenamed("value", "i94port_value")\
        .join(i94mode_df, i94_immigration_df.i94mode == i94mode_df.code, "inner")\
        .drop("code").withColumnRenamed("value", "i94mode_value")\
        .join(i94addr_df, i94_immigration_df.i94addr == i94addr_df.code, "inner")\
        .drop("code").withColumnRenamed("value", "i94addr_value")\
        .join(i94visa_df, i94_immigration_df.i94visa == i94visa_df.code, "inner")\
        .drop("code").withColumnRenamed("value", "i94visa_value")
    print(silver_i94_immigration_df.printSchema())
    silver_i94_immigration_df.write.format("delta").mode("overwrite").save("s3a://silver-layer-udacity-nd/i94_immigration")
