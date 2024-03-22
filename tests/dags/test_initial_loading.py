import pytest
from os.path import join, dirname, abspath

from airflow.models import DagBag


@pytest.fixture(scope = "package")
def dagbag():
    path = join(
        dirname(dirname(dirname(abspath(__file__)))), 
        "dags"
    )
    return DagBag(dag_folder = path, include_examples = False)

@pytest.fixture(scope = "module")
def dag(dagbag):
    return dagbag.get_dag(dag_id = "initial_loading")


def test_dag_loaded(dagbag, dag):
    assert dagbag.import_errors == {}
    assert dag is not None


def test_dag_struct(dag):
    expected_dag_struct = {
        "create_hive_tbls": ["load_dimension_data"],
        "load_dimension_data": [],
    }   # Downstream list of each task

    assert dag.task_dict.keys() == expected_dag_struct.keys()
    for task_id, expected_downstream in expected_dag_struct.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(expected_downstream)