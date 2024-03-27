from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from include.custom_spark.spark_submit import CustomSparkSubmitOperator
from datetime import datetime
from os import path


@dag(
    dag_id = "daily_clickstream_etl",
    schedule = None,
    tags = ["etl", "daily", "clickstream"],
    start_date = datetime(2018, 1, 1),
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
)
def daily_clickstream_etl():
    # Generate daily clickstream data
    generate_clickstream = CustomSparkSubmitOperator(
        task_id = "generate_daily_clickstream", 
        conn_id = "spark_default",
        application = path.join(
            path.dirname(path.dirname(__file__)),
            "jobs", "daily_clickstream_generate.py"
        ),
        name = "Generate daily user clickstream data"
    )


    # Transform and load aggregated daily clickstream data into DWH
    transform_clickstream = CustomSparkSubmitOperator(
        task_id = "transform_daily_clickstream", 
        conn_id = "spark_default",
        application = path.join(
            path.dirname(path.dirname(__file__)),
            "jobs", "daily_clickstream_transform.py"
        ),
        name = "Aggregate daily user clickstream data into DWH"
    )


    generate_clickstream >> transform_clickstream


run = daily_clickstream_etl()


if __name__ == "__main__":
    import argparse
    from include.local_config.dag_test_config import local_test_configs
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "execution_date",
        default = datetime.now(),
        help = "Date of DAG run (ISO format)"
    )
    args = parser.parse_args()
    args.execution_date = datetime.fromisoformat(args.execution_date)

    run.test(execution_date = args.execution_date, **local_test_configs)