import pyspark
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def quality_check(df: DataFrame, not_null: list, unique_key: list):
    """Check rows count, not null and unique key columns conditions

    Args:
        df (DataFrame): spark dataframe to analyse
        not_null (list): list of columns that can't be null
        unique_key (list): list of columns that should be unique
    """
    assert df.count() > 0
    assert df.count() == df.na.drop(subset=not_null).count()
    assert (
        df.groupBy(unique_key).count().filter(F.col("count") > 1).count() == 0
    )


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

    # final check
    not_null = [
        "origin_country",
        "resident_country",
        "application_state",
        "application_city",
        "arrival_date",
        "i94mode_value",
        "i94visa_value",
        "count",
    ]
    unique_key = [
        "origin_country",
        "resident_country",
        "application_state",
        "application_city",
        "arrival_date",
        "i94mode_value",
        "i94visa_value",
    ]
    immigration_df = spark.read.format("delta").load(
        "s3a://gold-layer-udacity-nd/grouped_immigration_data"
    )
    quality_check(immigration_df, not_null, unique_key)
