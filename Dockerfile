FROM apache/airflow:2.3.3

# Añadir el comando para instalar yfinance
RUN pip install yfinance
