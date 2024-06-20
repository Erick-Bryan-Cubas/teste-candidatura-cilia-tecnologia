import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def extract_data():
    # Dados de conexão com o banco de dados
    db_host = 'postgres'
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'

    # Crie a conexão com o banco de dados
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}')

    # Defina a consulta SQL
    query = """
    SELECT *
    FROM covid_data
    """

    # Execute a consulta e carregue os dados em um DataFrame
    df = pd.read_sql(query, engine)

    # Salve os dados extraídos em um arquivo CSV
    df.to_csv('/opt/airflow/data/extracted_data.csv', index=False)

if __name__ == "__main__":
    extract_data()
