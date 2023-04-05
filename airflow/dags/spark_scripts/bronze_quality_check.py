import pyspark
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def quality_check(df: DataFrame, not_null:list, unique_key:list):
    """Check rows count, not null and unique key columns conditions

    Args:
        df (DataFrame): spark dataframe to analyse
        not_null (list): list of columns that can't be null
        unique_key (list): list of columns that should be unique
    """
    assert df.count() > 0
    assert df.count() == df.na.drop(subset=not_null).count()
    assert df.groupBy(unique_key).count().filter(F.col("count") > 1).count() == 0


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.2.2")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    # airport check
    not_null = ["ident", "coordinates", "type", "name"]
    unique_key = ["ident", "coordinates"]
    airport_codes_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/airport_codes')
    quality_check(airport_codes_df, not_null, unique_key)

    # city check
    not_null = ["city", "state", "race", "total_population"]
    unique_key = ["city", "state", "race"]
    us_city_demographic_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/us_city_demographic')
    quality_check(us_city_demographic_df, not_null, unique_key)

    # i94_immigration check
    not_null = ["cicid", "i94yr", "arrdate", "arrdate"]
    unique_key = ["cicid"]
    i94_immigration_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94_immigration')
    quality_check(i94_immigration_df, not_null, unique_key)

    # i94 code/value tables
    not_null = ["code", "value"]
    unique_key = ["code", "value"]
    i94cit_res_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94cit_res')
    quality_check(i94cit_res_df, not_null, unique_key)
    i94port_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94port')
    quality_check(i94port_df, not_null, unique_key)
    i94mode_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94mode')
    quality_check(i94mode_df, not_null, unique_key)
    i94addr_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94addr')
    quality_check(i94addr_df, not_null, unique_key)
    i94visa_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/i94visa')
    quality_check(i94visa_df, not_null, unique_key)

    # temperature check
    not_null = ["dt", "city", "country", "latitude", "longitude"]
    unique_key = ["dt", "city", "country", "latitude", "longitude"]
    temperatures_df = spark.read.format("delta").load('s3a://bronze-layer-udacity-nd/world_temperature')
    quality_check(temperatures_df, not_null, unique_key)
