#TODO: 
# - Use MongoClient and utils.faker_custom_provider to ingest data into MongoDB
# - Use Spotify tracks dataset to get available track_ids 
# and use it as sample for faking data
# (https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset)
#       - Download dataset and use Airflow to save track_id column as another file
#       - When generating data, read data from that file

import pendulum

from airflow.models.dag import DAG
from airflow.decorators import dag, task


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}


@dag(
    dag_id = "daily_user_clickstream_etl",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2018, 1, 1, tz = "UTC")
)
def daily_user_clickstream_etl():
    pass


dag = daily_user_clickstream_etl()