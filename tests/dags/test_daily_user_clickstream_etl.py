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


def test_dag_struct(dag):
    expected_dag_struct = {
        "get_client_credentials": ["get_auth_msg"],
        "get_auth_msg": ["get_genres"],
        "get_genres": []
    }   # Downstream list of each task

    # Using dag.task_dict == expected_dag_struct will fail the test
    # because dag maps task_id (str) -> Task objects
    # while expected_dag_struct maps task_id (str) -> task_ids (List[str])
    # Hence we assert by keys first and then get each task_id in 
    # each Task object and compare
    assert dag.task_dict.keys() == expected_dag_struct.keys()
    for task_id, expected_downstream in expected_dag_struct.items():
        print(task_id)
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(expected_downstream)