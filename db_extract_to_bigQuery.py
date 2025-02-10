import os
import pyodbc
import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine
from db.db_manager import dbManager

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

db = dbManager(os.getenv('DRIVER'), os.getenv('SERVER'), os.getenv('DATABASE'))
print('Conectando...')
conn = db.connect()
cursor = conn.cursor()
query = 'SELECT * FROM Ventas_info'
try:

    cursor.execute(query)
    result = cursor.fetchall()
    print('Consulta existosa.')
except Exception as e:
    print(f'Error al ejecutar consulta {e}')
finally:
    cursor.close()
    conn.close()


ventas_df = pd.DataFrame(result)
print(ventas_df)
