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
                    yeni_list.append(tuple_hali)
                    gorulen.append(anahtar)

        hook = PostgresHook(postgres_conn_id='postgres')
        hook.run('TRUNCATE TABLE stg_carts')

        hook.insert_rows(
            table='stg_carts',
            rows=yeni_list,
            target_fields=['id', 'date', 'userId', 'quantity', 'productId']
        )

    user_data = users_check()
    extracted_users = extract_user(user_data)
    process_users(extracted_users)

    product_data = product_check()
    extracted_products = extract_products(product_data)
    process_products(extracted_products)

    carts_data = carts_check()
    extracted_carts = extract_carts(carts_data)
    process_carts(extracted_carts)

    create_user_table >> user_data
    create_products_table >> product_data
    create_table >> carts_data


fake_store_pipeline()