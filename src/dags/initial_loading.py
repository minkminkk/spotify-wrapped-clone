from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from include.custom_spark.spark_submit import CustomSparkSubmitOperator
from os import path


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
    template_searchpath = path.join(
        path.dirname(__file__), "include"
    )
)
def initial_loading():
    # Create Hive tables
    hive_tbls = SparkSqlOperator(
        task_id = "create_hive_tbls",
        conn_id = "spark_default",
        # master = spark://spark-master:7077,
            # conn master URL does not work properly so need explicitly stated
        name = "Create Hive tables",
        # conf = "spark.hive.metastore.uris=thrift://hive-metastore:9083",
        verbose = False,    # True by default
        sql = "sql/spark_tbls_init.sql"
    )


    # Initial loading Spark job
    load_dimension_data = CustomSparkSubmitOperator(
        task_id = "load_dimension_data", 
        conn_id = "spark_default",
        application = path.join(
            path.dirname(path.dirname(__file__)),
            "jobs", "load_dim_tbls.py"
        ),
        name = "Load dimension data"
    )


    hive_tbls >> load_dimension_data


run = initial_loading()


if __name__ == "__main__":
    # To test DAG locally; python dag_file.py
    from include.local_config.dag_test_config import local_test_configs
    
    run.test(**local_test_configs)