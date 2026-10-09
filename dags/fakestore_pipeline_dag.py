from airflow.sdk import dag, task, PokeReturnValue
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta, datetime
import logging
from psycopg.types.json import Jsonb as Json

# True: mock users + mock products test | False: full API -> staging -> dims -> facts DAG
TEST_MODE = False

@dag
def fake_store_pipeline():
    create_table = SQLExecuteQueryOperator(retries=2,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_table',
        conn_id='postgres',
        sql=''' CREATE TABLE IF NOT EXISTS stg_carts(
        id int,
        date timestamp,
        quantity int,
        userId int,
        productId int
        ) ''')

    create_products_table = SQLExecuteQueryOperator(retries=2,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_products_table',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS stg_products(
        title text,
        id numeric,
        description text,
        price numeric,
        category varchar(255),
        rate numeric,
        count int,
        image text
        )''')

    create_user_table = SQLExecuteQueryOperator(retries=2,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_user_table',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS stg_user(
        id int,
        email text,
        username text,
        phone text,
        firstname text,
        lastname text,
        city text,
        street text,
        number int,
        zip text
        )''')

    create_reject_table = SQLExecuteQueryOperator(retries=2,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_reject_table',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS reject_table(
        record_id numeric,
        raw_data jsonb,
        reason text,
        rejected_at timestamp,
        source text
        )''')


    create_audit_table = SQLExecuteQueryOperator(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(seconds=20),
        task_id='create_audit_table',
        conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS audit_table (
            run_at TIMESTAMP,
            source TEXT,
            extracted INT,
            accepted INT,
            rejected INT,
            status TEXT
        )
        '''
    )

    # --- Şema: 3 dim create'inden önce çalışması gereken tek task ---
    create_schema = SQLExecuteQueryOperator(retries=2,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_schema',
        conn_id='postgres',
        sql='CREATE SCHEMA IF NOT EXISTS fakestore_warehouse')

    # --- Dimension tabloları: 3 AYRI task, her biri kendi DDL'i ---
    create_date_dim = SQLExecuteQueryOperator(retries=2 ,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_date_dim',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS fakestore_warehouse.date_dim (
        "date" timestamp NULL,
        date_id int4 GENERATED ALWAYS AS IDENTITY,
        CONSTRAINT date_dim_pkey PRIMARY KEY (date_id)
        )''')

    create_product_dim = SQLExecuteQueryOperator(retries=2,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_product_dim',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS fakestore_warehouse.product_dim (
        productid int4 NULL,
        product_key int4 GENERATED ALWAYS AS IDENTITY,
        title text NULL,
        description text NULL,
        category text NULL,
        rate numeric NULL,
        count int4 NULL,
        CONSTRAINT product_dim_pkey PRIMARY KEY (product_key)
        )''')

    create_user_dim = SQLExecuteQueryOperator(retries=2,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_user_dim',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS fakestore_warehouse.user_dim (
        user_key int4 GENERATED ALWAYS AS IDENTITY,
        userid int4 NULL,
        username text NULL,
        firstname text NULL,
        lastname text NULL,
        phone text NULL,
        street text NULL,
        zipcode text NULL,
        "number" int4 NULL,
        email text NULL,
        city text NULL,
        CONSTRAINT user_dim_pkey PRIMARY KEY (user_key)
        )''')


    create_facts_a = SQLExecuteQueryOperator(retries=2 ,retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=20),
        task_id='create_facts_a',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS fakestore_warehouse.facts_a (
        quantity int4 NULL,
        price numeric NULL,
        cart_line_key int4 GENERATED ALWAYS AS IDENTITY,
        product_key int4 NULL,
        user_key int4 NULL,
        date_id int4 NULL,
        CONSTRAINT facts_a_pkey PRIMARY KEY (cart_line_key)
     )''')

    @task.sensor(poke_interval=20,timeout=120,retries=2, retry_delay=timedelta(minutes=2))
    def users_check() -> PokeReturnValue:
        import requests
        logging.info('Checking users API status code')
        try:
            response = requests.get('https://fakestoreapi.com/users', timeout=10)
            if response.status_code == 200:
                condition = True
                user = response.json()
                logging.info('Users API status code is valid')
            else:
                condition = False
                user = None
                logging.warning('Users API status code is not valid')
        except requests.exceptions.RequestException:
            condition = False
            user = None
            logging.warning('Users API did not respond, will poke again')

        return PokeReturnValue(is_done=condition, xcom_value=user)

    @task.sensor(poke_interval=20,timeout=120,retries=2, retry_delay=timedelta(minutes=2))
    def product_check() -> PokeReturnValue:
        import requests
        logging.info('Checking products API status code')
        try:
            response = requests.get('https://fakestoreapi.com/products', timeout=10)
            if response.status_code == 200:
                condition = True
                user = response.json()
                logging.info('Products API status code is valid')
            else:
                condition = False
                user = None
                logging.warning('Products API status code is not valid')
        except requests.exceptions.RequestException:
            condition = False
            user = None
            logging.warning('Products API did not respond, will poke again')

        return PokeReturnValue(is_done=condition, xcom_value=user)

    @task.sensor(poke_interval=20,timeout=120,retries=2, retry_delay=timedelta(minutes=2))
    def carts_check() -> PokeReturnValue:
        import requests
        logging.info('Checking carts API status code')
        try:
            response = requests.get('https://fakestoreapi.com/carts', timeout=10)
            if response.status_code == 200:
                condition = True
                user = response.json()
                logging.info('Carts API status code is valid')
            else:
                condition = False
                user = None
                logging.warning('Carts API status code is not valid')
        except requests.exceptions.RequestException:
            condition = False
            user = None
            logging.warning('Carts API did not respond, will poke again')

        return PokeReturnValue(is_done=condition, xcom_value=user)



    @task
    def product_check_data():
        valid_product = { 'rating':{ 'rate':5,'count':3},
                         'id':2, 'title':'oyuncu', 'price':32,
                         'category':'game', 'image': 'bmel',
                         'description':'gos'
                          }


        invalid_product = { 'rating':{ 'rate':2,'count':1},
                         'id':None, 'title':'oyuncu', 'price':23,
                         'category':'gt', 'image': 'ops',
                         'description':'gn'
                          }


        duplicate_product = { 'rating':{ 'rate':5,'count':3},
                                 'id':2, 'title':'oyuncu', 'price':32,
                                 'category':'game', 'image': 'bmel',
                                 'description':'gos'
                                  }


        return [valid_product,invalid_product,duplicate_product]


    @task
    def user_check_data():
        valid_user = {'id':2, 'email': 'burak@gmail.com',
                      'username':'anissa', 'phone':'123456789' ,'name':{'firstname':'burak','lastname':'coskun'},
                     'address':{  'city':'berlin','street':'neukoln',
                                                        'number':10,  'zipcode':'12043'}
                                              }


        invalid_user = {'id':None, 'email':'burak@gmail.com',
                        'username':'anissa', 'phone':'123456789','name':{'firstname':'burak','lastname':'coskun'},
                        'address':{  'city':'berlin','street':'neukoln',
                                   'number':10,  'zipcode':'12043'}
                         }

        duplicate_user = {'id':2, 'email': 'burak@gmail.com',
                      'username':'anissa', 'phone':'123456789' ,'name':{'firstname':'burak','lastname':'coskun'},
                     'address':{  'city':'berlin','street':'neukoln',
                                                        'number':10,  'zipcode':'12043'}}





        return [valid_user,invalid_user,duplicate_user]


    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_user(fake_user):
        return fake_user

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_products(fake_product):
        return fake_product

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_carts(fake_carts):
        return fake_carts

    @task(retries=2, retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=60))
    def process_products(fake_product):
        yeni = []
        reddedilen = []
        gorulen = []
        logging.info(f'{len(fake_product)} product records, starting transformation and validation')
        extracted_products = len(fake_product)
        for product in fake_product:
            rating = product['rating']
            rate = rating['rate']
            count = rating['count']
            product['rate'] = rate
            product['count'] = count
            del product['rating']

            if product['id'] not in gorulen:
                tuple_hali = (
                    product['id'],
                    product['title'],
                    product['price'],
                    product['category'],
                    product['rate'],
                    product['count'],
                    product['image'],
                    product['description']
                )
                if None not in tuple_hali:
                    yeni.append(tuple_hali)
                    gorulen.append(product['id'])
                else:
                    if product['id'] is None:
                        reason = 'product_id_is_null'
                    elif product['title'] is None:
                        reason = 'title_is_null'
                    elif product['price'] is None:
                        reason = 'price_is_null'
                    elif product['category'] is None:
                        reason = 'category_is_null'
                    elif product['rate'] is None:
                        reason = 'rate_is_null'
                    elif product['count'] is None:
                        reason = 'count_is_null'
                    elif product['image'] is None:
                        reason = 'image_is_null'
                    elif product['description'] is None:
                        reason = 'description_is_null'

                    reddedilen.append((
                        product['id'], Json(product), reason,
                        datetime.now(), 'products'
                    ))
            else:
               reddedilen.append((product['id'],Json(product),'duplicate',datetime.now(),'products'))
        accepted_products = len(yeni)
        rejected_products = extracted_products - accepted_products

        logging.info(
            f'Product reconciliation | '
            f'extracted={extracted_products} | '
            f'accepted={accepted_products} | '
            f'rejected={rejected_products}'
        )



        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute('TRUNCATE TABLE stg_products')
            cur.executemany('INSERT INTO stg_products (id, title, price, category, rate, count, image, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', yeni)
            cur.execute('SELECT COUNT(*) FROM stg_products')
            loaded_products = cur.fetchone()[0]
            if loaded_products != accepted_products:
                raise ValueError(
                    f'stg_products count mismatch | '
                    f'accepted={accepted_products} | loaded={loaded_products}'
                )
            cur.execute("DELETE FROM reject_table WHERE source='products'")
            cur.executemany('INSERT INTO reject_table (record_id, raw_data, reason, rejected_at, source) VALUES (%s, %s, %s, %s, %s)', reddedilen)
            cur.execute('INSERT INTO audit_table (run_at,source,extracted,accepted,rejected,status) VALUES (%s,%s,%s,%s,%s,%s)',
            (datetime.now(), 'products', extracted_products, accepted_products, rejected_products, 'success')
            )
            conn.commit()
        except Exception:
            conn.rollback()
            cur.execute('INSERT INTO audit_table (run_at,source,extracted,accepted,rejected,status) VALUES (%s,%s,%s,%s,%s,%s)',
            (datetime.now(), 'products', extracted_products, accepted_products, rejected_products, 'failed')
            )
            conn.commit()
            raise
        finally:
            conn.close()

        logging.info(
            f'{loaded_products} cleaned product records loaded into stg_products'
        )

    @task(retries=2, retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=60))
    def process_users(fake_user):
        list_yeni = []
        reddedilen = []
        gorulen = []
        logging.info(f'{len(fake_user)} user records, starting transformation and validation')
        extracted_count = len(fake_user)

        for user in fake_user:
            name = user['name']
            firstname = name['firstname']
            lastname = name['lastname']
            user['firstname'] = firstname
            user['lastname'] = lastname

            address = user['address']
            city = address['city']
            number = address['number']
            street = address['street']
            zipcode = address['zipcode']
            user['city'] = city
            user['number'] = number
            user['street'] = street
            user['zip'] = zipcode

            if user['id'] not in gorulen:
                tuple_hali = (
                    user['id'],
                    user['email'],
                    user['username'],
                    user['phone'],
                    user['firstname'],
                    user['lastname'],
                    user['city'],
                    user['street'],
                    user['number'],
                    user['zip']
                )
                if None not in tuple_hali:
                    list_yeni.append(tuple_hali)
                    gorulen.append(user['id'])
                else:
                    if user['id'] is None:
                        reason = 'user_id_is_null'
                    elif user['email'] is None:
                        reason = 'email_is_null'
                    elif user['username'] is None:
                        reason = 'username_is_null'
                    elif user['phone'] is None:
                        reason = 'phone_is_null'
                    elif user['firstname'] is None:
                        reason = 'firstname_is_null'
                    elif user['lastname'] is None:
                        reason = 'lastname_is_null'
                    elif user['city'] is None:
                        reason = 'city_is_null'
                    elif user['street'] is None:
                        reason = 'street_is_null'
                    elif user['number'] is None:
                        reason = 'number_is_null'
                    elif user['zip'] is None:
                        reason = 'zip_is_null'

                    reddedilen.append((
                        user['id'], Json(user), reason,
                        datetime.now(), 'users'
                    ))
            else:  # YENİ
                reddedilen.append((user['id'], Json(user), 'duplicate', datetime.now(), 'users'))

        accepted_user = len(list_yeni)
        rejected_user = extracted_count - accepted_user

        logging.info(
            f'User reconciliation | '
            f'extracted={extracted_count} | '
            f'accepted={accepted_user} | '
            f'rejected={rejected_user}'
        )


        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute('TRUNCATE TABLE stg_user')
            cur.executemany('INSERT INTO stg_user (id, email, username, phone, firstname, lastname, city, street, number, zip) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', list_yeni)
            cur.execute('SELECT COUNT(*) FROM stg_user')
            loaded_user = cur.fetchone()[0]
            if loaded_user != accepted_user:
                raise ValueError(
                    f'stg_user count mismatch | '
                    f'accepted={accepted_user} | loaded={loaded_user}'
                )
            cur.execute("DELETE FROM reject_table WHERE source='users'")  # YENİ
            cur.executemany('INSERT INTO reject_table (record_id, raw_data, reason, rejected_at, source) VALUES (%s, %s, %s, %s, %s)', reddedilen)  # YENİ
            cur.execute('INSERT INTO audit_table (run_at,source,extracted,accepted,rejected,status) VALUES (%s,%s,%s,%s,%s,%s)',
            (datetime.now(), 'users', extracted_count, accepted_user, rejected_user, 'success')
            )
            conn.commit()
        except Exception:
            conn.rollback()
            cur.execute('INSERT INTO audit_table (run_at,source,extracted,accepted,rejected,status) VALUES (%s,%s,%s,%s,%s,%s)',
            (datetime.now(), 'users', extracted_count, accepted_user, rejected_user, 'failed')
            )
            conn.commit()
            raise
        finally:
            conn.close()

        logging.info(f'{loaded_user} cleaned user records loaded into stg_user')

    @task(retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(seconds=60))
    def process_carts(fake_carts):
        yeni_list = []
        gorulen = []
        reddedilen = []

        logging.info(
            f'{len(fake_carts)} cart records, starting transformation and validation'
        )

        extracted_cart_lines = 0

        for cart in fake_carts:
            extracted_cart_lines += len(cart['products'])

            for cart_line in cart['products']:
                anahtar = (cart['id'], cart_line['productId'])

                if anahtar not in gorulen:
                    tuple_hali = (
                        cart['id'],
                        cart['date'],
                        cart['userId'],
                        cart_line['quantity'],
                        cart_line['productId']
                    )

                    if None not in tuple_hali:
                        yeni_list.append(tuple_hali)
                        gorulen.append(anahtar)
                    else:
                        if cart_line['productId'] is None:
                            reason = 'product_id_is_null'
                        elif cart_line['quantity'] is None:
                            reason = 'quantity_is_null'
                        elif cart['id'] is None:
                            reason = 'cart_id_is_null'
                        elif cart['date'] is None:
                            reason = 'cart_date_is_null'
                        elif cart['userId'] is None:
                            reason = 'user_id_is_null'

                        reddedilen.append((cart['id'], Json(cart_line), reason, datetime.now(), 'carts'))
                else:
                    reddedilen.append((cart['id'], Json(cart_line), 'duplicate', datetime.now(), 'carts'))

        accepted_cart_lines = len(yeni_list)
        rejected_cart_lines = extracted_cart_lines - accepted_cart_lines

        logging.info(
            f'Cart reconciliation | '
            f'extracted={extracted_cart_lines} | '
            f'accepted={accepted_cart_lines} | '
            f'rejected={rejected_cart_lines}'
        )

        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()

        try:
            cur = conn.cursor()

            cur.execute('TRUNCATE TABLE stg_carts')

            cur.executemany(
                '''
                INSERT INTO stg_carts
                (id, date, userId, quantity, productId)
                VALUES (%s, %s, %s, %s, %s)
                ''',
                yeni_list
            )

            cur.execute('SELECT COUNT(*) FROM stg_carts')
            loaded_cart_lines = cur.fetchone()[0]
            if loaded_cart_lines != accepted_cart_lines:
                raise ValueError(
                    f'stg_carts count mismatch | '
                    f'accepted={accepted_cart_lines} | loaded={loaded_cart_lines}'
                )
            cur.execute("DELETE FROM   reject_table  WHERE SOURCE = 'carts'")
            cur.executemany('INSERT INTO  reject_table (record_id, raw_data, reason, rejected_at, source) VALUES (%s, %s, %s, %s, %s)', reddedilen)
            cur.execute(
                '''INSERT INTO audit_table
                (run_at, source, extracted, accepted, rejected, status)
                VALUES (%s, %s, %s, %s, %s, %s)''',
                (datetime.now(), 'carts', extracted_cart_lines, accepted_cart_lines, rejected_cart_lines, 'success')
            )
            conn.commit()

        except Exception:
            conn.rollback()

            cur.execute('''INSERT INTO audit_table  (run_at,source,extracted,accepted,rejected,status)
                        VALUES(%s,%s,%s,%s,%s,%s)''',
                        (datetime.now(), 'carts', extracted_cart_lines,accepted_cart_lines, rejected_cart_lines, 'failed'))
            conn.commit()
            raise

        finally:
            conn.close()

        logging.info(
            f'{loaded_cart_lines} cleaned cart line records loaded into stg_carts'
        )


    @task(retries=2, retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=40))
    def fill_product_dim():
        logging.info('Starting product_dim load')
        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute('TRUNCATE TABLE fakestore_warehouse.product_dim')
            cur.execute('''
                INSERT INTO fakestore_warehouse.product_dim
                (productid, title, description, category, rate, count)
                SELECT id, title, description, category, rate, count
                FROM stg_products
            ''')
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logging.info('product_dim load completed successfully')

    @task(retries=2, retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=40))
    def fill_user_dim():
        logging.info('Starting user_dim load')
        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute('TRUNCATE TABLE fakestore_warehouse.user_dim')
            cur.execute('''
                INSERT INTO fakestore_warehouse.user_dim
                (userid, username, firstname, lastname, phone, street, zipcode, number, email, city)
                SELECT id, username, firstname, lastname, phone, street, zip, number, email, city
                FROM stg_user
            ''')
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logging.info('user_dim load completed successfully')


    @task(retries=2, retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=120))
    def fill_facts():
        logging.info('Starting facts_a load')
        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute('TRUNCATE TABLE fakestore_warehouse.facts_a')
            cur.execute('''
                INSERT INTO fakestore_warehouse.facts_a
                (quantity, price, product_key, user_key, date_id)
                SELECT quantity, price, product_key, user_key, date_id
                FROM stg_carts
                JOIN fakestore_warehouse.product_dim ON stg_carts.productId = product_dim.productid
                JOIN stg_products ON stg_carts.productId = stg_products.id
                JOIN fakestore_warehouse.user_dim ON stg_carts.userId = user_dim.userid
                JOIN fakestore_warehouse.date_dim ON stg_carts.date = date_dim.date
            ''')
            cur.execute('SELECT COUNT(*) FROM stg_carts')
            staged_cart_lines = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM fakestore_warehouse.facts_a')
            loaded_facts = cur.fetchone()[0]

            logging.info(
                f'Facts reconciliation | '
                f'stg_carts={staged_cart_lines} | '
                f'facts_a={loaded_facts} | '
                f'lost={staged_cart_lines - loaded_facts}'
            )

            if loaded_facts != staged_cart_lines:
                raise ValueError(
                    f'facts_a count mismatch | '
                    f'stg_carts={staged_cart_lines} | facts_a={loaded_facts}'
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logging.info('facts_a load completed successfully')

    @task(retries=2, retry_delay=timedelta(minutes=2),execution_timeout=timedelta(seconds=40))
    def fill_date_dim():
        logging.info('Starting date_dim load')
        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute('TRUNCATE TABLE fakestore_warehouse.date_dim')
            cur.execute('''
                INSERT INTO fakestore_warehouse.date_dim
                (date)
                SELECT DISTINCT date FROM stg_carts
            ''')
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logging.info('date_dim load completed successfully')




    if TEST_MODE:
        # USERS TEST (mock data, API yok)
        test_user = user_check_data()
        b = process_users(test_user)

        create_user_table >> b
        create_reject_table >> b
        create_audit_table >> b

        # PRODUCTS TEST (mock data, API yok)
        test_product = product_check_data()
        processed_products = process_products(test_product)

        create_products_table >> processed_products
        create_reject_table >> processed_products
        create_audit_table >> processed_products

    else:
        # FULL DAG: API -> extract -> staging -> dimensions -> facts
        users_data = extract_user(users_check())
        products_data = extract_products(product_check())
        carts_data = extract_carts(carts_check())

        loaded_users = process_users(users_data)
        loaded_products = process_products(products_data)
        loaded_carts = process_carts(carts_data)

        # staging tablolari yazmadan once hazir olmali
        create_user_table >> loaded_users
        create_products_table >> loaded_products
        create_table >> loaded_carts

        # reject ve audit tablolari: uc process task'i da bunlara yaziyor
        create_reject_table >> loaded_users
        create_reject_table >> loaded_products
        create_reject_table >> loaded_carts
        create_audit_table >> loaded_users
        create_audit_table >> loaded_products
        create_audit_table >> loaded_carts

        # dim ve fact tablolari fakestore_warehouse semasinda
        create_schema >> create_user_dim
        create_schema >> create_product_dim
        create_schema >> create_date_dim
        create_schema >> create_facts_a

        user_dimension_load = fill_user_dim()
        product_dimension_load = fill_product_dim()
        date_dimension_load = fill_date_dim()
        facts_load = fill_facts()

        # dim doldurma: tablo olusmus + staging dolmus olmali
        create_user_dim >> user_dimension_load
        loaded_users >> user_dimension_load

        create_product_dim >> product_dimension_load
        loaded_products >> product_dimension_load

        create_date_dim >> date_dimension_load
        loaded_carts >> date_dimension_load

        # fact: tablo + stg_carts + uc dim'in surrogate key'leri hazir olmali
        create_facts_a >> facts_load
        loaded_carts >> facts_load
        user_dimension_load >> facts_load
        product_dimension_load >> facts_load
        date_dimension_load >> facts_load


fake_store_pipeline()