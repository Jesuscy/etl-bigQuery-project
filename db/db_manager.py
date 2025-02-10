import os
from dotenv import load_dotenv
import pyodbc

load_dotenv()


class dbManager():

    def __init__(self, driver, server, db):
        self.driver = driver
        self.server = server
        self.db = db

    def connect(self):
        # Construye la cadena de conexión
        conn_str = (f"Driver={os.getenv('DRIVER')};"
                    f"Server={os.getenv('SERVER')};"
                    f"DATABASE={os.getenv('DATABASE')};"
                    "Trusted_Connection=yes;")
        try:
            conn = pyodbc.connect(conn_str)
            print("Conexión exitosa")
        except pyodbc.Error as e:
            print(f"Error al conectar: {e}")




# Crea una instancia de la clase y llama al método connect()
db = dbManager(os.getenv('DRIVER'), os.getenv('SERVER'), os.getenv('DATABASE'))
db.connect()
