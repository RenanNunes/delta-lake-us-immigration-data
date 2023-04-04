from datetime import datetime

from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessStartJobOperator,
)

from airflow import DAG
from airflow.models import Variable

# Replace these with your correct values
JOB_ROLE_ARN = Variable.get("emr_serverless_job_role")
S3_LOGS_BUCKET = Variable.get("emr_serverless_log_bucket")

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://{S3_LOGS_BUCKET}/logs/"}
    },
}

with DAG(
    dag_id="elt_us_immigration_data",
    schedule_interval="0 7 * * *",
    start_date=datetime(2021, 1, 1),
    tags=["elt", "daily", "immigration", "weather", "airport", "demographics"],
    catchup=False,
) as dag:
    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app",
        job_type="SPARK",
        release_label="emr-6.10.0",
        config={
            "name": "sample-job",
            },
    )

    application_id = create_app.output

    # Bronze layer
    bronze_airports = EmrServerlessStartJobOperator(
        task_id="bronze_airports",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://utils-bucket-udacity/spark_files/bronze_airports.py",
                "sparkSubmitParameters": "--conf spark.jars=s3://utils-bucket-udacity/jars/delta-core_2.12-2.2.0.jar,s3://utils-bucket-udacity/jars/delta-storage-2.2.0.jar"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )


    bronze_cities = EmrServerlessStartJobOperator(
        task_id="bronze_cities",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://utils-bucket-udacity/spark_files/bronze_cities.py",
                "sparkSubmitParameters": "--conf spark.jars=s3://utils-bucket-udacity/jars/delta-core_2.12-2.2.0.jar,s3://utils-bucket-udacity/jars/delta-storage-2.2.0.jar"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )


    bronze_immigration = EmrServerlessStartJobOperator(
        task_id="bronze_immigration",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://utils-bucket-udacity/spark_files/bronze_immigration.py",
                "sparkSubmitParameters": "--conf spark.jars=s3://utils-bucket-udacity/jars/delta-core_2.12-2.2.0.jar,s3://utils-bucket-udacity/jars/delta-storage-2.2.0.jar"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )


    bronze_temperatures = EmrServerlessStartJobOperator(
        task_id="bronze_temperatures",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://utils-bucket-udacity/spark_files/bronze_temperatures.py",
                "sparkSubmitParameters": "--conf spark.jars=s3://utils-bucket-udacity/jars/delta-core_2.12-2.2.0.jar,s3://utils-bucket-udacity/jars/delta-storage-2.2.0.jar --conf spark.sql.parquet.int96RebaseModeInWrite=CORRECTED"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )


    bronze_i94_values = EmrServerlessStartJobOperator(
        task_id="bronze_i94_values",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://utils-bucket-udacity/spark_files/bronze_i94_values.py",
                "sparkSubmitParameters": "--conf spark.jars=s3://utils-bucket-udacity/jars/delta-core_2.12-2.2.0.jar,s3://utils-bucket-udacity/jars/delta-storage-2.2.0.jar"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )


    # acabar de colocar as tasks da bronze, fazer os scripts .py delas e colocar no s3 (fazer um commit com essa layer)

    # Silver layer


    # Gold layer


    # Check quality


    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    (create_app >> [bronze_airports, bronze_cities, bronze_immigration, bronze_temperatures, bronze_i94_values] >> delete_app)
