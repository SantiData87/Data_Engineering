#El objetivo de esta entrega es extraer los datos de la API de yahoo finance que me permitan evaluar los precios de cierre de las acciones denominadas las 7 Magnificas
#Una vez obtenidos los datos cargarlos a Amazon Redshift

#pip install yfinance
#Importo Bibliotecas Basicas
import pandas as pd
import numpy as np
#importar yfinance (Previa instalacion de la API: pip install yfinance) para tener datos de mercado financiero
import yfinance as yf
#Importar librerias de Visualizacion
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns


#1) Obtengo los registros de la API-------------------------------------------------------------------------------------------------------------------------
#1.1) Creo una lista en la que incluyo los tickers de las Acciones denominadas 7 Maginifcas
tickers = ['MSFT','GOOG','AAPL','AMZN', 'META', 'TSLA', 'NVDA']

# Descargo el precio de Cierre de las Acciones antes mencionada
data = {}
for ticker in tickers:
    data[ticker] = yf.download(ticker, period="10Y", interval="1d")['Close']

# Creo DataFrame 
df = pd.DataFrame(data
                  )
#Imprimo el DataFrame, sus columnas y tipos de datos contenidos en las mismas
print(df)
print(df.columns)
print('---------------------------------------------------------------------------------------------------------------------------------------')

# Mover el índice a la primera columna y reorganizar las columnas
df.reset_index(inplace=True)  # Reiniciar el índice y mover Date a una columna
df = df[['Date', 'MSFT', 'GOOG', 'AAPL', 'AMZN', 'META', 'TSLA', 'NVDA']]  # Reorganizar las columnas según sea necesario
df['Date'] = pd.to_datetime(df['Date'])  # Convertir la columna Date a tipo datetime

print(df)
print(df.columns)
print(df.dtypes)
print('---------------------------------------------------------------------------------------------------------------------------------------')

#Cargo el DataFrame como CSV
#df.to_csv('C:/Users/Sofia Medici/Desktop/DATA/Py Notebooks/Entregables/entregable_1_Santiago_Hourcade.csv', index=False)

#2) Cargo la tabla a Redshift------------------------------------------------------------------------------------------------------------------------
#2.1) Creando la conexión a Redsshift
import psycopg2
host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="cpn_santiago_hourcade_coderhouse"
pwd="5iQ2iPP1I4"
try:
    conn = psycopg2.connect(
        host=host,
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print("Conectado a Redshift con éxito!")
    
except Exception as e:
    print("No es posible conectar a Redshift")
    print(e)


#2.2) Crear la tabla si no existe
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cpn_santiago_hourcade_coderhouse.siete_magnificas
        (
        Date DATE PRIMARY KEY,
        MSFT VARCHAR(100),
        GOOG VARCHAR(100),
        AAPL VARCHAR(100),
        AMZN VARCHAR(100),
        META VARCHAR(100),
        TSLA VARCHAR(100),
        NVDA VARCHAR(100)     
        )
    """)
    conn.commit()

#2.2.1) Vaciar la tabla para evitar duplicados o inconsistencias
with conn.cursor() as cur:
  cur.execute("Truncate table siete_magnificas")
  count = cur.rowcount

#2.2.2) Consultando que la tabla este vacia
cur = conn.cursor()
cur.execute("SELECT * FROM cpn_santiago_hourcade_coderhouse.siete_magnificas")
results = cur.fetchall()
print(results)



#2.3) Insertando los datos en Redsfhift
from psycopg2.extras import execute_values
with conn.cursor() as cur:
    execute_values(
        cur,
        '''
        INSERT INTO siete_magnificas (Date, MSFT, GOOG, AAPL, AMZN, META, TSLA, NVDA)
        VALUES %s
        ''',
        [tuple(row) for row in df.values],
        page_size=len(df)
    )
    conn.commit()



#3) Cierro Conexion-----------------------------------------------------------------------------------------------------------------------------
conn.close()
cur.close()
