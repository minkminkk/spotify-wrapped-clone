import pendulum
import logging

from airflow.utils.edgemodifier import Label
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from include.custom_hooks_operators.spark_submit \
    import CustomSparkSubmitOperator


@dag(
    dag_id = "daily_user_clickstream_etl",
    schedule = "@daily",
    start_date = pendulum.datetime(2024, 1, 1, tz = "UTC"),
    tags = ["etl", "daily", "clickstream"],
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0
    }
)
def daily_user_clickstream_etl():
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


    # TODO: Maybe replace bunch of SQLOperator with SQLHook?
    # Default SparkSQL kwargs
    default_spark_sql_kwargs = {
        "conn_id": "spark_default",
        "master": "spark://spark-master:7077",
        "conf": "spark.hive.metastore.uris=thrift://hive-metastore:9083"
    }


    hive_tbls = SparkSqlOperator(
        task_id = "create_hive_tbls",
        sql = "/opt/airflow/dags/include/spark_tbls_init.sql",
        name = "Create Hive tables",
        **default_spark_sql_kwargs
    )   #TODO: Fix sql cannot recognize .sql file


    cnt_tracks = SparkSqlOperator(
        task_id = "get_cnt_tracks",
        conn_id = "spark_default",
        sql = "SELECT COUNT(*) cnt FROM dim_tracks LIMIT 1;",
        master = "spark://spark-master:7077",
        name = "Get tracks count",
        conf = "spark.hive.metastore.uris=thrift://hive-metastore:9083"
    )

    cnt_artists = SparkSqlOperator(
        task_id = "get_cnt_artists",
        sql = "SELECT COUNT(*) cnt FROM dim_artists LIMIT 1;",
        name = "Get artists count",
        **default_spark_sql_kwargs
    )

    cnt_users = SparkSqlOperator(
        task_id = "get_cnt_users",
        sql = "SELECT COUNT(*) cnt FROM dim_users LIMIT 1;",
        name = "Get users count",
        **default_spark_sql_kwargs
    )

    cnt_dates = SparkSqlOperator(
        task_id = "get_cnt_dates",
        sql = "SELECT COUNT(*) cnt FROM dim_dates LIMIT 1;",
        name = "Get dates count",
        **default_spark_sql_kwargs
    )


    load_dim_tbls = CustomSparkSubmitOperator(
        task_id = "load_dim_data",
        application = "/jobs/load_dim_tbls.py",
        name = "Load data for dimension tables",
        properties_file = "/jobs/spark-defaults.conf",
        application_args = ["-t", access_token, "-g", genres]
    )


    # Check if need to request new data or load more data to 
    # get enough dimension data
    @task.branch
    def need_request_api_data(cnt_tracks, cnt_artists):
        """Check if tables dim_tracks and dim_artists are empty.
        If any is empty, request new data and append new data only.
        """
        if cnt_tracks == 0 or cnt_artists == 0:
            return ["get_access_token"]
        else:
            return ["need_load_dim_data"]
    branch_request_data = need_request_api_data(cnt_tracks, cnt_artists)
    

    end_dag = EmptyOperator(task_id = "end_dag")


    @task.branch
    def need_load_dim_data(cnt_users, cnt_dates):
        """Need to load data if any dimension table (tracks, artists, 
        dates, users) is empty. Only need to check cnt_dates because previous 
        branch checked cnt_tracks and cnt_artists already.
        """
        if cnt_users == 0 or cnt_dates == 0:
            return ["load_dim_tbls"]
        else:
            return ["end_dag"]
    branch_load_dim = need_load_dim_data(cnt_users, cnt_dates)
        

    # Dependency specifications
    [cnt_tracks, cnt_artists] >> branch_request_data
    branch_request_data \
        >> Label("True (at least 1 table is empty)") \
        >> access_token >> genres
    branch_request_data \
        >> Label("False (both tables are non-empty)") \
        >> branch_load_dim
    
    hive_tbls >> [cnt_tracks, cnt_artists, cnt_users, cnt_dates]
    [cnt_users, cnt_dates] >> branch_load_dim
    branch_load_dim \
        >> Label("True (at least 1 table is empty)") \
        >> load_dim_tbls
    branch_load_dim \
        >> Label("False (all dimension tables have data)") \
        >> end_dag
    load_dim_tbls >> end_dag


run = daily_user_clickstream_etl()


if __name__ == "__main__":
    # To test DAG locally; python dag_file.py
    from include.config.dag_test_config import local_test_configs
    
    run.test(**local_test_configs)