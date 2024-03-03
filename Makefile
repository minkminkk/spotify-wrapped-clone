venv_dev:
	python -m venv .venv
	.venv/bin/activate
	pip install --no-cache-dir -r requirements-venv-dev.txt


up:
	docker compose up -d
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop


airflow_container=airflow
airflow_shell:
	docker exec -it ${airflow_container} /bin/bash
test_dag:
	docker exec -it ${airflow_container} airflow dags test scrape_spotify_api