import sys
sys.path.insert(0, '/opt/airflow/dags')
from airflow.models import DagBag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from fakestore_pipeline_dag import ayir_products, ayir_integrity, process_users
import pytest

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



def test_rollback():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute('TRUNCATE TABLE stg_user')
    kullanicilar = [
        (1, 'derek@gmail.com', 'derek', '1-111-111-1111', 'derek', 'powell', 'berlin', 'main st', 10, '10115'),
        (2, 'kate@gmail.com', 'kate_h', '1-222-222-2222', 'kate', 'hale', 'hamburg', 'oak ave', 20, '20095'),
    ]
    cur.executemany(
        'INSERT INTO stg_user (id, email, username, phone, firstname, lastname, city, street, number, zip) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
        kullanicilar)

    conn.commit()


    kullanicilar_hata = [
        {'id': 3, 'email': 'burak@gmail.com',
         'username': 'anissa', 'phone': '123456789',
         'name': {'firstname': 'brk', 'lastname': 'coskun'},
         'address': {'city': 'berlin', 'street': 'neukoln',
                     'number': 'um', 'zipcode': '12043'}}
    ]


    with pytest.raises(Exception):
        process_users.function(kullanicilar_hata)



    cur.execute('SELECT COUNT(*) FROM stg_user')
    sayi = cur.fetchone()[0]
    cur.execute('SELECT * FROM stg_user')
    eslik = cur.fetchall()
    assert sayi == len(kullanicilar)
    assert eslik == kullanicilar
    conn.close()

