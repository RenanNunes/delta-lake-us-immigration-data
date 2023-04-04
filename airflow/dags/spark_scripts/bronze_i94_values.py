import pyspark
from pyspark.sql.types import StructType, StructField, StringType

def generate_spark_dataframe_from_file(spark, file_path, header_identifier):
    file_content = "\n".join(spark.sparkContext.textFile(file_path).collect())
    start_of_values = file_content.index(header_identifier)
    end_of_values = file_content.index(";", start_of_values)

    searched_values = file_content[start_of_values:end_of_values]

    schema = StructType([
        StructField('code', StringType(), True),
        StructField('value', StringType(), True)
    ])
    values = []

    for line in searched_values.split("\n"):
        if line.find("=") != -1:
            breaked_line = line.split("=")
            values.append([breaked_line[0].strip(" '\t"), breaked_line[1].strip(" '\t")])

        values_dataframe = spark.createDataFrame(values, schema)

    return values_dataframe

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.2.2")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    I94cit_res_df = generate_spark_dataframe_from_file(
        spark, "s3a://landing-layer-udacity-nd/I94_SAS_Labels_Descriptions.SAS", "I94CIT")
    print(I94cit_res_df.printSchema())
    I94cit_res_df.write.format("delta").mode("overwrite").save("s3a://bronze-layer-udacity-nd/i94cit_res")
    
    I94port_df = generate_spark_dataframe_from_file(
        spark, "s3a://landing-layer-udacity-nd/I94_SAS_Labels_Descriptions.SAS", "I94PORT")
    print(I94port_df.printSchema())
    I94port_df.write.format("delta").mode("overwrite").save("s3a://bronze-layer-udacity-nd/i94port")

    I94mode_df = generate_spark_dataframe_from_file(
        spark, "s3a://landing-layer-udacity-nd/I94_SAS_Labels_Descriptions.SAS", "I94MODE")
    print(I94mode_df.printSchema())
    I94mode_df.write.format("delta").mode("overwrite").save("s3a://bronze-layer-udacity-nd/i94mode")

    I94addr_df = generate_spark_dataframe_from_file(
        spark, "s3a://landing-layer-udacity-nd/I94_SAS_Labels_Descriptions.SAS", "I94ADDR")
    print(I94addr_df.printSchema())
    I94addr_df.write.format("delta").mode("overwrite").save("s3a://bronze-layer-udacity-nd/i94addr")

    I94visa_df = generate_spark_dataframe_from_file(
        spark, "s3a://landing-layer-udacity-nd/I94_SAS_Labels_Descriptions.SAS", "I94VISA")
    print(I94visa_df.printSchema())
    I94visa_df.write.format("delta").mode("overwrite").save("s3a://bronze-layer-udacity-nd/i94visa")
