import os
import pyodbc
import pandas as pd
from datetime import datetime
from db.db_manager import dbManager
from dotenv import load_dotenv

load_dotenv()

# Formateo el csv al formato de la tabla en BD.
ventas_df = pd.read_csv('./dataSet/supermarket_sales.csv')
ventas_df = ventas_df.dropna()

ventas_df['Tax 5%'] = ventas_df['Tax 5%'].apply(lambda x: round(x, 2))
ventas_df['Total'] = ventas_df['Total'].apply(lambda x: round(x, 2))
ventas_df['cogs'] = ventas_df['cogs'].apply(lambda x: round(x, 2))
ventas_df['gross margin percentage'] = ventas_df['gross margin percentage'].apply(
    lambda x: round(x, 2))
ventas_df['gross income'] = ventas_df['gross income'].apply(
    lambda x: round(x, 2))
ventas_df['Date'] = pd.to_datetime(ventas_df['Date'], format='%m/%d/%Y')
ventas_df['Time'] = pd.to_datetime(
    ventas_df['Time'], format='%H:%M', errors='coerce')

ventas_df.columns = ['InvoiceID', 'Branch', 'City', 'CustomerType', 'Gender', 'ProductLine', 'UnitPrice', 'Quantity',
                     'Tax', 'Total', 'Date', 'Time', 'Payment', 'Cogs', 'GrossMarginPercentage', 'GrossIncome', 'Rating']

print(ventas_df)
ventas_df.info()
print('Modificaciones DataFrame realizadas.')

# Me conecto a la BD y ejecuto la subida.
print('Conectando a BD ...')
db = dbManager(os.getenv('DRIVER'), os.getenv('SERVER'), os.getenv('DATABASE'))
conn = db.connect()

if conn:
    print('Conexión exitosa.')
    cursor = conn.cursor()

    for row in ventas_df.itertuples(index=False):
        values = (
            row.InvoiceID, row.Branch, row.City, row.CustomerType, row.Gender,
            row.ProductLine, row.UnitPrice, row.Quantity, row.Tax, row.Total,
            row.Date, row.Time, row.Payment, row.Cogs, row.GrossMarginPercentage,
            row.GrossIncome, row.Rating
        )

        query = ('''INSERT INTO Ventas_info (InvoiceID, Branch, City, CustomerType, Gender, ProductLine, 
                    UnitPrice, Quantity, Tax, Total, Date, Time, Payment, Cogs, GrossMarginPercentage, 
                    GrossIncome, Rating) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''')

        cursor.execute(query, values)

    conn.commit()
    cursor.close()
    conn.close()
else:
    print('Error al conectar a la base de datos')
