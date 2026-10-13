import sys
sys.path.insert(0, '/opt/airflow/dags')
from airflow.models import DagBag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from fakestore_pipeline_dag import ayir_products, ayir_integrity


dagBag = DagBag(dag_folder='/opt/airflow/dags')

def test_airflow():
    assert len(dagBag.import_errors) == 0
    assert 'fake_store_pipeline' in dagBag.dag_ids


def  test_duplicate():
    valid_product = {'rating': {'rate': 5, 'count': 3},
                     'id': 2, 'title': 'oyuncu', 'price': 32,
                     'category': 'game', 'image': 'bmel',
                     'description': 'gos'}

    duplicate_product = {'rating': {'rate': 5, 'count': 3},
                         'id': 2, 'title': 'oyuncu', 'price': 32,
                         'category': 'game', 'image': 'bmel',
                         'description': 'gos'}

    urunler = [valid_product,duplicate_product]


    yeni , reddedilen = ayir_products(urunler)

    assert len(yeni)== 1
    assert len(reddedilen) == 1
    assert reddedilen[0][2] =='duplicate'


def test_null():

    valid_product = {'rating': {'rate': 5, 'count': 3},
                     'id': 2, 'title': 'oyuncu', 'price': 32,
                     'category': 'game', 'image': 'bmel',
                     'description': 'gos'}

    invalid_product = {'rating': {'rate': 2, 'count': 1},
                       'id': None, 'title': 'oyuncu', 'price': 23,
                       'category': 'gt', 'image': 'ops',
                       'description': 'gn'}


    urunler_d = [valid_product, invalid_product]

    yeni, reddedilen = ayir_products(urunler_d)

    assert len(yeni)==1
    assert len(reddedilen) == 1
    assert reddedilen[0][2] == 'product_id_is_null'


def test_integrity():
    conn = PostgresHook(postgres_conn_id='postgres').get_conn()
    cur = conn.cursor()

    try:
        cur.execute('''
            INSERT INTO stg_carts (id, date, quantity, userid, productid)
            VALUES (99999, '2026-01-01', 1, 1, 99999)
        ''')

        ayir_integrity(conn)

        cur.execute("SELECT COUNT(*) FROM reject_table WHERE record_id = 99999 AND reason = 'unmatched_product'")
        sayi = cur.fetchone()[0]
        assert sayi == 1

    finally:
        conn.rollback()
        conn.close()