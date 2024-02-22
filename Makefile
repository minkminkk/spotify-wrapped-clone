venv_dev:
	python -m venv .venv
	.venv/bin/activate
	pip install --no-cache-dir -r requirements-venv-dev.txt


repo_dirs:
	mkdir -p ./logs/airflow \
		& sudo chmod 777 -R ./logs


up: repo_dirs
	docker compose up -d
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop


airflow_container=spotify-wrapped-clone-airflow-1
airflow_shell:
	docker exec -it ${airflow_container} /bin/bash
test_dag:
	docker exec -it ${airflow_container} airflow dags test scrape_spotify_api