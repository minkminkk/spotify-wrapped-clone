#TODO: 
# - Use MongoClient and utils.faker_custom_provider to ingest data into MongoDB
# - Use Spotify tracks dataset to get available track_ids 
# and use it as sample for faking data
# (https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset)
#       - Download dataset and use Airflow to save track_id column as another file
#       - When generating data, read data from that file

from typing import List
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.decorators.setup_teardown import setup_task, teardown_task
from airflow.models.variable import Variable


@dag(
    dag_id = "daily_user_clickstream_etl",
    schedule = "@daily",
    start_date = pendulum.datetime(2018, 1, 1, tz = "UTC"),
    tags = ["etl"],
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0
    }
)
def daily_user_clickstream_etl():
    pass

    
run = daily_user_clickstream_etl()


if __name__ == "__main__":
    # To test DAG locally; python dag_file.py
    from include.dag_test_config import local_test_configs
    
    run.test(**local_test_configs)