from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from include.custom_spark.spark_submit import CustomSparkSubmitOperator
from os import path


@dag(
    dag_id = "daily_user_clickstream_etl",
    schedule = None,
    tags = ["etl", "daily", "clickstream"],
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
)
def daily_user_clickstream_etl():
    # Initial loading Spark job
    generate_clickstream = CustomSparkSubmitOperator(
        task_id = "generate_daily_clickstream", 
        conn_id = "spark_default",
        application = path.join(
            path.dirname(path.dirname(__file__)),
            "jobs", "generate_daily_clickstream.py"
        ),
        name = "Generate daily user clickstream data"
    )


run = daily_user_clickstream_etl()


if __name__ == "__main__":
    # To test DAG locally; python dag_file.py
    from include.local_config.dag_test_config import local_test_configs
    
    run.test(**local_test_configs)