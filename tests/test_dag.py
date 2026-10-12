from airflow.models import DagBag

dagBag = DagBag(dag_folder='/opt/airflow/dags')

def test_airflow():
    assert len(dagBag.import_errors) == 0
    assert 'fake_store_pipeline' in dagBag.dag_ids