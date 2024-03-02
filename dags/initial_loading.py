import logging

from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from include.custom_hooks_operators.spark_submit \
    import CustomSparkSubmitOperator
from airflow.utils.edgemodifier import Label


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
    from pyspark import SparkContext    # type-check
    from pyspark.sql import SparkSession    # type-check


    hive_tbls = SparkSqlOperator(
        task_id = "create_hive_tbls",
        conn_id = "spark_default",
        master = "spark://spark-master:7077",
        name = "Create Hive tables",
        conf = "spark.hive.metastore.uris=thrift://hive-metastore:9083",
            # because SparkSQL does not check spark-defaults.conf
        sql = "spark_tbls_init.sql"
    )


    @task.pyspark(
        conn_id = "spark_default",
        config_kwargs = {"spark-master": "spark://spark-master:7077"}
    )
    def check_tbl_empty(spark: SparkSession, sc: SparkContext):
        tbls = ("dim_artists", "dim_tracks", "dim_users", "dim_dates")
        empty = {t: spark.table(t).isEmpty() for t in tbls}
        return empty
    empty_res = check_tbl_empty()


    # Check if need to request new data or load more data to 
    # get enough dimension data
    @task.branch
    def decide_tbl_load(empty_res: dict[str, bool]):
        """If dim_tracks or dim_artists empty, request_data and load_dim.
        Elif dim_users or dim_dates empty, load_dim only.
        Else (all tables non-empty), end DAG.

        load_dim Spark job already exclude loading for non-empty tables. 
        """

        if empty_res["is_dim_tracks_empty"] \
            or empty_res["is_dim_artists_empty"]:
            return ["get_access_token"]
        elif empty_res["is_dim_users_empty"] \
            or empty_res["is_dim_dates_empty"]:
            return ["load_dim_tbls"]
        else:
            return ["end_dag"]
    branch_tbl_load = decide_tbl_load(empty_res)


    @task
    def get_access_token(client_id, client_secret):
        """client_id, client_secret is templated via variables 
        and raise error if not exist.
        """
        from spotify_api_client.auth \
            import ClientAuthenticator, ClientCredentialsStrategy

        # Generate auth object and get access token
        auth = ClientAuthenticator(client_id, client_secret)
        auth.set_strategy(ClientCredentialsStrategy())

        return auth.get_access_token()
    

    @task(show_return_value_in_logs = False)
    def get_genres(access_token: dict):
        from spotify_api_client.session import APISession
        
        with APISession(access_token) as session:
            logging.info("Requesting genre data")
            r_json = session.get_genres()
            
            genres = r_json["genres"]
            logging.info(f"SUMMARY: Retrieved {len(genres)} genres")

        return genres

    access_token = get_access_token(
        client_id = "{{ var.value.get('spotify_webapi_client_id') }}",
        client_secret = "{{ var.value.get('spotify_webapi_client_secret') }}"
    )
    genres = get_genres(access_token)


    load_dim_tbls = CustomSparkSubmitOperator(
        task_id = "load_dim_tbls",
        application = "/jobs/load_dim_tbls.py",
        name = "Load data for dimension tables",
        properties_file = "/jobs/spark-defaults.conf",
        application_args = ["-t", access_token, "-g", genres]
    )
    

    end_dag = EmptyOperator(task_id = "end_dag")


    # Task dependency specifications
    hive_tbls >> empty_res >> branch_tbl_load
    branch_tbl_load \
        >> Label("dim_tracks or dim_artists empty") \
        >> access_token >> genres \
        >> Label("Request tracks and/or artists data") \
        >> load_dim_tbls
    branch_tbl_load \
        >> Label("dim_users or dim_dates empty") \
        >> load_dim_tbls
    branch_tbl_load \
        >> Label("All tables are non-empty") \
        >> end_dag
    load_dim_tbls >> end_dag


run = daily_user_clickstream_etl()


if __name__ == "__main__":
    # To test DAG locally; python dag_file.py
    from include.config.dag_test_config import local_test_configs
    
    run.test(**local_test_configs)