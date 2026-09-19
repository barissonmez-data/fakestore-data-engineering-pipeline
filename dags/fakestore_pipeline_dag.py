from airflow.sdk import dag, task, PokeReturnValue
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta
import logging

@dag
def fake_store_pipeline():
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        sql=''' CREATE TABLE IF NOT EXISTS stg_carts(
        id int,
        date timestamp,
        quantity int,
        userId int,
        productId int
        ) ''')

    create_products_table = SQLExecuteQueryOperator(
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

    create_user_table = SQLExecuteQueryOperator(
        task_id='create_user_tablo',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS stg_user(
        id int,
        email text,
        username text,
        password text,
        phone text,
        firstname text,
        lastname text,
        city text,
        street text,
        number int,
        zip text
        )''')

    # --- Şema: 3 dim create'inden önce çalışması gereken tek task ---
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id='postgres',
        sql='CREATE SCHEMA IF NOT EXISTS fakestore_warehouse')

    # --- Dimension tabloları: 3 AYRI task, her biri kendi DDL'i ---
    create_date_dim = SQLExecuteQueryOperator(
        task_id='create_date_dim',
        conn_id='postgres',
        sql='''CREATE TABLE IF NOT EXISTS fakestore_warehouse.date_dim (
        "date" timestamp NULL,
        date_id int4 GENERATED ALWAYS AS IDENTITY,
        CONSTRAINT date_dim_pkey PRIMARY KEY (date_id)
        )''')

    create_product_dim = SQLExecuteQueryOperator(
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

    create_user_dim = SQLExecuteQueryOperator(
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


    create_facts_a = SQLExecuteQueryOperator(
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

    @task.sensor(retries=2, retry_delay=timedelta(minutes=2))
    def users_check() -> PokeReturnValue:
        import requests
        logging.info(f'check status code')
        response = requests.get('https://fakestoreapi.com/users')
        if response.status_code == 200:
            condition = True
            user = response.json()
            logging.info(f'status code is valid')
        else:
            condition = False
            user = None
            logging.warning(f'status code is not valid')
        return PokeReturnValue(is_done=condition, xcom_value=user)

    @task.sensor(retries=2, retry_delay=timedelta(minutes=2))
    def product_check() -> PokeReturnValue:
        import requests
        logging.info(f'check status code')
        response = requests.get('https://fakestoreapi.com/products')
        if response.status_code == 200:
            condition = True
            user = response.json()
            logging.info(f'status code is valid')
        else:
            condition = False
            user = None
            logging.warning('status code is not valid')

        return PokeReturnValue(is_done=condition, xcom_value=user)

    @task.sensor(retries=2, retry_delay=timedelta(minutes=2))
    def carts_check() -> PokeReturnValue:
        logging.info(f'check status code')
        import requests
        response = requests.get('https://fakestoreapi.com/carts')
        if response.status_code == 200:
            condition = True
            user = response.json()
            logging.info(f'status code is valid')
        else:
            condition = False
            user = None
            logging.warning(f'status code is not valid')

        return PokeReturnValue(is_done=condition, xcom_value=user)

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_user(fake_user):
        return fake_user

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_products(fake_product):
        return fake_product

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_carts(fake_carts):
        return fake_carts

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def process_products(fake_product):
        yeni = []
        gorulen = []

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

        hook = PostgresHook(postgres_conn_id='postgres')
        hook.run('TRUNCATE TABLE stg_products')
        hook.insert_rows(
            table='stg_products',
            rows=yeni,
            target_fields=['id', 'title', 'price', 'category', 'rate', 'count', 'image', 'description']
        )

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def process_users(fake_user):
        list_yeni = []
        gorulen = []

        for user in fake_user:
            name = user['name']
            firstname = name['firstname']
            lastname = name['lastname']
            user['firstname'] = firstname
            user['lastname'] = lastname
            del user['name']

            address = user['address']
            city = address['city']
            number = address['number']
            street = address['street']
            zipcode = address['zipcode']
            user['city'] = city
            user['number'] = number
            user['street'] = street
            user['zip'] = zipcode
            del user['address']

            del user['__v']

            if user['id'] not in gorulen:
                tuple_hali = (
                    user['id'],
                    user['email'],
                    user['username'],
                    user['password'],
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

        hook = PostgresHook(postgres_conn_id='postgres')
        hook.run('TRUNCATE TABLE stg_user')
        hook.insert_rows(
            table='stg_user',
            rows=list_yeni,
            target_fields=['id', 'email', 'username', 'password', 'phone', 'firstname', 'lastname', 'city', 'street', 'number', 'zip']
        )

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def process_carts(fake_carts):
        yeni_list = []
        gorulen = []

        for cart in fake_carts:
            for product in cart['products']:
                anahtar = (cart['id'], product['productId'])

                if anahtar not in gorulen:
                    tuple_hali = (
                        cart['id'],
                        cart['date'],
                        cart['userId'],
                        product['quantity'],
                        product['productId']
                    )
                    if None not in tuple_hali:
                        yeni_list.append(tuple_hali)
                        gorulen.append(anahtar)

        hook = PostgresHook(postgres_conn_id='postgres')
        hook.run('TRUNCATE TABLE stg_carts')

        hook.insert_rows(
            table='stg_carts',
            rows=yeni_list,
            target_fields=['id', 'date', 'userId', 'quantity', 'productId']
        )

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def fill_product_dim():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.run('TRUNCATE TABLE fakestore_warehouse.product_dim')
        hook.run('''
               INSERT INTO fakestore_warehouse.product_dim
              (productid, title, description, category, rate, count)

               SELECT id, title, description, category, rate, count

               FROM stg_products

        ''')

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def fill_user_dim():
      hook = PostgresHook(postgres_conn_id='postgres')
      hook.run('TRUNCATE TABLE fakestore_warehouse.user_dim')
      hook.run('''
        INSERT INTO fakestore_warehouse.user_dim
        (userid, username, firstname, lastname, phone, street, zipcode, number, email, city)
        SELECT id, username, firstname, lastname, phone, street, zip, number, email, city
        FROM stg_user
    ''')


    @task(retries=2, retry_delay=timedelta(minutes=2))
    def fill_facts():
      hook = PostgresHook(postgres_conn_id='postgres')
      hook.run('TRUNCATE TABLE fakestore_warehouse.facts_a')
      hook.run('''
        INSERT INTO fakestore_warehouse.facts_a
        (quantity, price, product_key, user_key, date_id)
        SELECT quantity, price, product_key, user_key, date_id
        FROM stg_carts
        JOIN fakestore_warehouse.product_dim ON stg_carts.productId = product_dim.productid
        JOIN stg_products ON stg_carts.productId = stg_products.id
        JOIN fakestore_warehouse.user_dim ON stg_carts.userId = user_dim.userid
        JOIN fakestore_warehouse.date_dim ON stg_carts.date = date_dim.date
    ''')

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def fill_date_dim():
      hook = PostgresHook(postgres_conn_id='postgres')
      hook.run('TRUNCATE TABLE fakestore_warehouse.date_dim')
      hook.run('''
        INSERT INTO fakestore_warehouse.date_dim
        (date)
        SELECT DISTINCT date FROM stg_carts
    ''')

    user_data = users_check()
    extracted_users = extract_user(user_data)

    product_data = product_check()
    extracted_products = extract_products(product_data)

    carts_data = carts_check()
    extracted_carts = extract_carts(carts_data)

    # staging create'ler -> ilgili extraction/sensor
    create_user_table >> user_data
    create_products_table >> product_data
    create_table >> carts_data

    # Her fill fonksiyonu TEK KERE çağrılıp değişkene atanıyor.
    # Aynı fonksiyonu iki kez çağırmak Airflow'da iki AYRI task (__1 ekiyle) oluşturur.
    product_filled = fill_product_dim()
    user_filled = fill_user_dim()
    date_filled = fill_date_dim()

    # staging fill zinciri (extraction -> fill)
    process_products(extracted_products) >> product_filled
    process_users(extracted_users) >> user_filled
    process_carts(extracted_carts) >> date_filled

    # şema -> 3 dim create (birbirinden bağımsız, sadece şemayı bekler)
    create_schema >> create_date_dim
    create_schema >> create_product_dim
    create_schema >> create_user_dim

    # dim create -> ilgili dim fill (aynı task'a ikinci bağımlılık)
    create_product_dim >> product_filled
    create_user_dim >> user_filled
    create_date_dim >> date_filled

    facts_filled = fill_facts()

    [product_filled, user_filled, date_filled] >> facts_filled
    create_schema >> create_facts_a
    create_facts_a >> facts_filled

fake_store_pipeline()