.PHONY: up down start stop venv_dev


# Settings
venv_dir=.venv
namenode_container=namenode
airflow_container=airflow


# Create local virtual environment
venv_dev:
	python -m venv ${venv_dir}
	${venv_dir}/bin/activate
	pip install --no-cache-dir -r requirements-venv-dev.txt


# Docker compose shortcuts
up:
	docker compose up -d
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop


# Setup data for namenode
download_data:
	wget --output-document ./data/spotify-tracks.parquet \
		https://huggingface.co/api/datasets/maharshipandya/spotify-tracks-dataset/parquet/default/train/0.parquet
setup_namenode: download_data
	docker exec ${namenode_container} hdfs dfs -mkdir -p /data_lake
	docker exec ${namenode_container} hdfs dfs -mkdir -p /data_warehouse
	docker exec ${namenode_container} hdfs dfs -put \
		./data/spotify-tracks.parquet /data_lake/spotify-tracks.parquet


# Shortcuts for Airflow container
airflow_shell:
	docker exec -it ${airflow_container} /bin/bash
test_dag:
	docker exec -it ${airflow_container} airflow dags test scrape_spotify_api

spark_file=/jobs/load_dim_tbls_from_dataset.py
test_job:
	docker exec -it ${airflow_container} spark-submit \
		--master spark://spark-master:7077 \
		--properties-file /jobs/spark-defaults.conf \
		${spark_file}