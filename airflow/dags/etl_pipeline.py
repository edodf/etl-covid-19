from datetime import datetime
import logging
import requests
import pandas as pd
import logging
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.python import PythonOperator
from connector import Connector
from transform import Transform    


def func_get_data_from_api(**kwargs):
    # get data
    response = requests.get(Variable.get('url_api'))
    result = response.json()
    data = pd.DataFrame(result['data']['content'])
    print(data.info())


    # create connectorju
    get_conn = Connection.get_connection_from_secrets("Mysql")
    connector = Connector()
    engine_sql = connector.conn_mysql(
        host = get_conn.host,
        user = get_conn.login,
        password = get_conn.password,
        db = get_conn.schema,
        port = get_conn.port
    )
    
    # drop table if exists
    try:
        drop_table = """DROP TABLE IF EXISTS covid"""
        engine_sql.execute(drop_table)
    except Exception as e:
        logging.error(e)

    # insert to mysql
    data.to_sql(con=engine_sql, name='covid', index=False)
    logging.info("[INFO] insert successful")
    # tampilkan data
    pd.set_option('display.max_columns', None)
    print(data)    

def func_generate_dim(**kwargs):
    # create connector
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # mysql conenctor
    connector = Connector()
    engine_sql = connector.conn_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    # postgres conenctor
    connector = Connector()
    engine_postgres = connector.conn_postgresql(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insert data
    transformer = Transform(engine_sql, engine_postgres)
    transformer.create_dimension_case()
    transformer.create_dimension_province()
    transformer.create_dimension_district()

def func_insert_province_daily(**kwargs):
    # create connector
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # mysql connector
    connector = Connector()
    engine_sql = connector.conn_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    # postgres connector
    connector = Connector()
    engine_postgres = connector.conn_postgresql(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transformer = Transform(engine_sql, engine_postgres)
    transformer.create_province_daily()

def func_insert_district_daily(**kwargs):
    # create connector
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # mysql connector
    connector = Connector()
    engine_sql = connector.conn_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    # postgres connector
    connector = Connector()
    engine_postgres = connector.conn_postgresql(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transformer = Transform(engine_sql, engine_postgres)
    transformer.create_district_daily()

with DAG(
    dag_id = 'etl',
    start_date = datetime(2023, 11, 27),
    schedule_interval = '0 0 * * *',
    catchup = False
) as dag:
        
    op_get_data_from_api = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable = func_get_data_from_api
    )

    op_generate_dim = PythonOperator(
        task_id = 'generate_dim',
        python_callable = func_generate_dim
    )

    op_insert_province_daily = PythonOperator(
        task_id = 'insert_province_daily',
        python_callable = func_insert_province_daily
    )

    op_insert_district_daily = PythonOperator(
        task_id = 'insert_district_daily',
        python_callable = func_insert_district_daily
    )

op_get_data_from_api >> op_generate_dim
op_generate_dim >> [op_insert_province_daily, op_insert_district_daily]
