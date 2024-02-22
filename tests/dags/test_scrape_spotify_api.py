import pytest
from os.path import dirname, abspath, join

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    path = join(dirname(dirname(dirname(abspath(__file__)))), "src", "dags")
    return DagBag(dag_folder = path, include_examples = False)

@pytest.fixture()
def dag(dagbag):
    return dagbag.get_dag(dag_id = "daily_user_clickstream_etl")


def test_dag_loaded(dagbag, dag):
    assert dagbag.import_errors == {}
    assert dag is not None