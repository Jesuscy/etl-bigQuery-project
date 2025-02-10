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
        conn_str = (f"Driver={self.driver};"
                    f"Server={self.server};"
                    f"DATABASE={self.db};"
                    "Trusted_Connection=yes;")
        try:
            conn = pyodbc.connect(conn_str)
            print("Conexión exitosa")
            return conn
        except pyodbc.Error as e:
            print(f"Error al conectar: {e}")
            return None

   # def close(self): 