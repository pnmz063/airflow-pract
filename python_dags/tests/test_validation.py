from airflow.models import DagBag
import pytest


def test_dag_on_description():
    dag_bag = DagBag(dag_folder='lect1/', include_examples=False)
    for dag in dag_bag.dags:
        descriptions = dag_bag.dags[dag].description
        print(descriptions)
        error_msg = f"Description not set for DAG {dag}"
        assert descriptions is None, error_msg


test_dag_on_description()

