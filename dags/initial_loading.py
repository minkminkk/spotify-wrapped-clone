from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from include.custom_spark.spark_submit import CustomSparkSubmitOperator


@dag(
    dag_id = "initial_loading",
    schedule = None,
    tags = ["etl", "daily", "clickstream"],
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    template_searchpath = "/opt/airflow/dags/include/sql"
)
def daily_user_clickstream_etl():
    # Create Hive tables
    hive_tbls = SparkSqlOperator(
        task_id = "create_hive_tbls",
        conn_id = "spark_default",
        master = "spark://spark-master:7077",
        name = "Create Hive tables",
        conf = "spark.hive.metastore.uris=thrift://hive-metastore:9083",
        verbose = False,    # True by default
        sql = "spark_tbls_init.sql"
    )


    # Initial loading Spark job
    initial_loading = CustomSparkSubmitOperator(
        task_id = "initial_loading", 
        conn_id = "spark_default"
    )


    hive_tbls >> initial_loading


run = daily_user_clickstream_etl()


if __name__ == "__main__":
    # To test DAG locally; python dag_file.py
    from include.local_config.dag_test_config import local_test_configs
    
    run.test(**local_test_configs)