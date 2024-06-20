import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def extract_data():
    # Configure a string de conexão
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    # Executa a consulta SQL
    query = "SELECT * FROM covid_data"
    df = pd.read_sql_query(query, engine)

    # Salva os dados em um arquivo CSV
    df.to_csv('/opt/airflow/data/extracted_data.csv', index=False)

if __name__ == "__main__":
    extract_data()
