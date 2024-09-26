import pytest
from airflow.models import DagBag

# Path to your DAG file
DAG_FILE = "path_to_your_dag_file"

@pytest.fixture
def dag_bag():
    return DagBag(dag_folder=DAG_FILE)

def test_dag_loaded(dag_bag):
    """
    Test if the DAG is properly loaded
    """
    dag_id = 'airflow_monitoring'
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None, "DAG failed to load"
    assert len(dag.tasks) == 1, "DAG should have one task"

def test_task_properties(dag_bag):
    """
    Test properties of the BashOperator task in the DAG
    """
    dag = dag_bag.get_dag('airflow_monitoring')
    task = dag.get_task('echo')
    assert task.bash_command == 'echo test', "bash_command is incorrect"
    assert task.priority_weight == 2**31 - 1, "priority_weight is incorrect"
