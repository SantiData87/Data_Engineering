# pip install yfinance

# Importo Bibliotecas Básicas
import pandas as pd
import numpy as np
# Importar yfinance para tener datos de mercado financiero
import yfinance as yf
# Importar librerias de DAGs
from datetime import datetime, timedelta
# Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
# SMTP
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from psycopg2.extras import execute_values

# 1) Obtengo los registros de la API
def download_data():
    # Creo una lista en la que incluyo los tickers de las Acciones denominadas 7 Magnificas
    tickers = ['MSFT', 'GOOG', 'AAPL', 'AMZN', 'META', 'TSLA', 'NVDA']

    # Descargo el precio de Cierre de las Acciones antes mencionadas
    data = {}
    for ticker in tickers:
        data[ticker] = yf.download(ticker, period="10Y", interval="1d")['Close']

    # Creo DataFrame 
    df = pd.DataFrame(data)
    
    # Mover el índice a la primera columna y reorganizar las columnas
    df.reset_index(inplace=True)
    df = df[['Date', 'MSFT', 'GOOG', 'AAPL', 'AMZN', 'META', 'TSLA', 'NVDA']]
    df['Date'] = pd.to_datetime(df['Date'])

    # Guardar el DataFrame como CSV (descomentar si es necesario)
    # df.to_csv('C:/Users/Sofia Medici/Desktop/DATA/Py Notebooks/EntregaFinal/entregable_1_Santiago_Hourcade.csv', index=False)
    
    df.to_csv('/tmp/entregable_1_Santiago_Hourcade.csv', index=False)  # Guardar en una ruta temporal

# 2) Cargo la tabla a Redshift
def load_redshift():
    df = pd.read_csv('/tmp/entregable_1_Santiago_Hourcade.csv')  # Leer desde la ruta temporal

    # Creando la conexión a Redshift
    host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base = "data-engineer-database"
    user = "cpn_santiago_hourcade_coderhouse"
    pwd = Variable.get("pwd_redshift")

    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conectado a Redshift con éxito!")
        
        # Crear la tabla si no existe
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

        # Vaciar la tabla para evitar duplicados o inconsistencias
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE cpn_santiago_hourcade_coderhouse.siete_magnificas")
            conn.commit()

        # Insertando los datos en Redshift
        with conn.cursor() as cur:
            execute_values(
                cur,
                '''
                INSERT INTO cpn_santiago_hourcade_coderhouse.siete_magnificas (Date, MSFT, GOOG, AAPL, AMZN, META, TSLA, NVDA)
                VALUES %s
                ''',
                [tuple(row) for row in df.values],
                page_size=len(df)
            )
            conn.commit()
            
    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
    
    finally:
        if conn:
            conn.close()

# 3) Envío aviso por email (SMTP) y Cierro Conexión
def SMTP_close_conexion():
    sender_email = Variable.get("email")
    sender_password = Variable.get("pwd_email")
    receiver_email = Variable.get("email")
    subject = "Aviso - Pipeline completado"
    body = "Los datos han sido actualizados con éxito"
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            text = msg.as_string()
            server.sendmail(sender_email, receiver_email, text)
            print("Email enviado con éxito")
    except Exception as e:
        print("Error: No se pudo enviar el email")
        print(e)

# 4) DAGS TAREAS

default_args = {
    'owner': 'SantiagoHourcade',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)  # 2 min de espera antes de cualquier reintento
}

api_dag = DAG(
    dag_id="Entrega_Final_Santiago_Hourcade_",
    default_args=default_args,
    description="DAG para tomar datos de la API de Yahoo Finance y vaciar datos en Redshift",
    start_date=datetime(2024, 7, 4, 2),
    schedule_interval='@daily' 
)

task1 = BashOperator(
    task_id='Iniciando_primera_tarea',
    bash_command='echo Iniciando...',
    dag=api_dag
)

task2 = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=api_dag,
)

task3 = PythonOperator(
    task_id='load_redshift',
    python_callable=load_redshift,
    dag=api_dag,
)

task4 = PythonOperator(
    task_id='SMTP_close_conexion',
    python_callable=SMTP_close_conexion,
    dag=api_dag,
)

task5 = BashOperator(
    task_id='Proceso_Completado_ultima_tarea',
    bash_command='echo Proceso completado...',
    dag=api_dag
)

task1 >> task2 >> task3 >> task4 >> task5
