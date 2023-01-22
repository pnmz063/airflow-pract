from airflow.models import DagBag


def test_dag_on_description():
    """
    тест дагов на наличия параметра description

    """
    dag_bag = DagBag(dag_folder='../dags/', include_examples=False)
    for dag in dag_bag.dags:
        descriptions = dag_bag.dags[dag].description
        error_msg = f"Description not set for DAG {dag}"
        assert descriptions is not None, error_msg


test_dag_on_description()

