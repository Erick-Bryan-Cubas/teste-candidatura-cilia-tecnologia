import psycopg2
import csv
import logging
import os
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_value(value):
    if value == '':
        return None
    return value

def fill_and_concat_city_state(reader):
    previous_city = None
    filled_rows = []
    for row in reader:
        if row[1]:
            previous_city = row[1]
        else:
            row[1] = f"{previous_city} - {row[16]}"
        filled_rows.append(row)
    return filled_rows

def load_csv_to_postgres(csv_file_path):
    try:
        # Conecte ao banco de dados
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="cilia-tecnologia",
            user="postgres",
            password="trojan19"
        )
        cur = conn.cursor()

        # Crie a tabela
        cur.execute("""
        CREATE TABLE IF NOT EXISTS covid_data (
            id SERIAL PRIMARY KEY,
            city TEXT,
            city_ibge_code FLOAT,
            date DATE,
            epidemiological_week INT,
            estimated_population FLOAT,
            estimated_population_2019 FLOAT,
            is_last BOOLEAN,
            is_repeated BOOLEAN,
            last_available_confirmed INT,
            last_available_confirmed_per_100k_inhabitants FLOAT,
            last_available_date DATE,
            last_available_death_rate FLOAT,
            last_available_deaths INT,
            order_for_place INT,
            place_type TEXT,
            state TEXT,
            new_confirmed INT,
            new_deaths INT,
            daily_variation FLOAT
        )
        """)
        conn.commit()

        # Carregar o arquivo CSV com a codificação correta
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Pular o cabeçalho
            filled_rows = fill_and_concat_city_state(reader)

            # Processar com tqdm para a barra de progresso
            for row in tqdm(filled_rows, desc="Loading data"):
                # Ignorar a primeira coluna "Unnamed: 0" e limpar valores
                cleaned_row = [clean_value(val) for val in row[1:]]
                cur.execute("""
                    INSERT INTO covid_data (
                        city, city_ibge_code, date, epidemiological_week,
                        estimated_population, estimated_population_2019, is_last,
                        is_repeated, last_available_confirmed,
                        last_available_confirmed_per_100k_inhabitants, last_available_date,
                        last_available_death_rate, last_available_deaths,
                        order_for_place, place_type, state, new_confirmed,
                        new_deaths, daily_variation
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, cleaned_row)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("CSV loaded into PostgreSQL successfully")

    except Exception as e:
        logger.error(f"Error loading CSV to PostgreSQL: {e}")

if __name__ == "__main__":
        # Determine o caminho absoluto do arquivo CSV
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # csv_file_path = os.path.join(base_dir, '..', 'airflow', 'data', 'file.csv')
    # csv_file_path = os.path.join(base_dir, '..', 'Dataset-2', 'file-minimal-data.csv')
    csv_file_path = os.path.join(base_dir, '..', 'Dataset-2', 'file.csv')
    load_csv_to_postgres(csv_file_path)
