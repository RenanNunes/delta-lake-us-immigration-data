# delta-lake-us-immigration-data
Repository for udacity capstone project. It uses Airflow, Delta Lake, S3 and python/pyspark to create bronze, silver and gold tables to analyse immigration data alongside temperature, demographic and airport code data. Generating a better quantitative understanding of arrivals across the years in the USA.

# Architecture
## Technologies
- **Airflow**: Open source technology to orquestrate the data flow of the application;
- **AWS EMR Serverless**: Serverless application to run Spark jobs, simplifying configuration and deploys;
- **AWS S3**: Storage service to easily store and retrieve data. In this case, it was used to store: spark files, data files (csv and parquets), logs, jars and delta tables;
- **Delta Lake**: Open source software that extends parquet files with log of transactions, allowing ACID transactions and time travels.

## Design
![Architecture design](/images/udacity-nanodegree-architecture.png)
### Airflow DAG
![Airflow DAG](/images/airflow-dag.png)

## S3 Buckets
- **utils-bucket-udacity**: bucket for spark jars, spark files and execution logs
- **landing-layer-udacity-nd**: bucket for raw files in any format (csv, txt, parquet, etc)
- **bronze-layer-udacity-nd**: bucket for bronze layer delta table
- **silver-layer-udacity-nd**: bucket for silver layer delta table
- **gold-layer-udacity-nd**: bucket for gold layer delta table

# Folder structure
- **Capstone Project Notebook.ipynb**: notebook with project summary, data description, data exploration, data modeling, quality check and some conceptual questions
- **airflow**: airflow folder with docker compose (based on [airflow "how to"](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)), dags and spark files
- **credentials-airflow-user.txt**: credentials for airflow user (next topic will explain how to create)
- **data**: datasets that are going to be used on the notebook

# How to run
## Create policy and role
Create a policy with the following JSON (or with more restrictive resource list (instead of `*`) if it is going to run on productive environment):
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "FullAccessToOutputBucket",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Sid": "EMRServerlessActions",
            "Effect": "Allow",
            "Action": [
                "emr-serverless:CreateApplication",
                "emr-serverless:UpdateApplication",
                "emr-serverless:DeleteApplication",
                "emr-serverless:ListApplications",
                "emr-serverless:GetApplication",
                "emr-serverless:StartApplication",
                "emr-serverless:StopApplication",
                "emr-serverless:StartJobRun",
                "emr-serverless:CancelJobRun",
                "emr-serverless:ListJobRuns",
                "emr-serverless:GetJobRun"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:GetRole",
                "iam:PassRole"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
```
Then create a role with this policy

## Create user
Create a user with the same policy and download the Access keys as `credentials-airflow-user.txt` in the format "<id>,<key>"

## Create buckets
Create the 5 buckets described before and populate the:
- utils bucket with delta-core jar (example: `delta-core_2.12-2.2.0.jar`), delta storage jar (example: `delta-storage-2.2.0.jar`) inside a `jars` folder, with spark files (that are inside `airflow/dags/spark_files`) inside a `spark_files` folder
- landing bucket with not zipped files inside `data` and with the zipped content after decompression

## Run docker compose
```
docker compose up
```

## Create variables and connection
Create the 2 variables:
- **emr_serverless_job_role**: arn for the role created before (example: arn:aws:iam::634541097731:role/emr_serverless)
- **emr_serverless_log_bucket**: utils bucket name created before (example: utils-bucket-udacity)
Create the AWS connection with the following information:
- Connection Id: aws_default
- Connection Type: Amazon Web Services
- AWS Access Key ID: key id from the airflow-user created
- AWS Secret Access Key: secret access key from the airflow-user created
- Extra: `{"region_name": "us-east-1"}` (or other region depending on the buckets location)

## Run the DAG
Login with `airflow` as login and password, then turn on the DAG and wait for completion
