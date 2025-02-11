import os
import time
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from db.db_manager import dbManager

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './cloud_cred.json'
credentials = service_account.Credentials.from_service_account_file(
    './cloud_cred.json',
    scopes=['https://www.googleapis.com/auth/bigquery']
)

db = dbManager(os.getenv('DRIVER'), os.getenv('SERVER'), os.getenv('DATABASE'))
print('Conectando...')
conn = db.connect()
cursor = conn.cursor()


def sql_extract():
    query = 'SELECT * FROM Ventas_info'
    try:

        ventas_df = pd.read_sql_query(query, conn)
        print(ventas_df.head())
        return ventas_df

    except Exception as e:
        print(f'Error al ejecutar consulta {e}')
        return e
    finally:
        cursor.close()
        conn.close()


def gcp_load():
    cliente = bigquery.Client(credentials=credentials)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition='WRITE_APPEND'
    )
    table_id = 'etl-bigquet.demo.supermarket_sales'
    job = cliente.load_table_from_dataframe(
        sql_extract(), table_id, job_config=job_config)

    while job.state != 'DONE':
        time.sleep(2)
        job.reload()

    print(job.result)
    table = cliente.get_table(table_id)
    print(
        f'Cargadas {table.num_rows} filas y {table.schema} columnas a la tabla {table.table_id}')


gcp_load()
