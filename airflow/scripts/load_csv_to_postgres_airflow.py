import psycopg2
import csv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dados de conexão com o banco de dados
db_host = 'postgres'
db_name = 'airflow'
db_user = 'airflow'
db_password = 'airflow'
logger.info("Starting Data Load")

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
        # Verificar se o arquivo existe
        logger.info("Checking if CSV file exists at %s", csv_file_path)
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            logger.info("CSV file found")
        
        # Conecte ao banco de dados
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        cur = conn.cursor()
        logger.info("Connected to PostgreSQL")

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
        logger.info("Table created")

        # Carregar o arquivo CSV com a codificação correta
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Pular o cabeçalho
            next(reader)
            filled_rows = fill_and_concat_city_state(reader)

            batch_size = 1000
            batch = []

            for row in filled_rows:
                # Ignorar a primeira coluna "Unnamed: 0" e limpar valores
                cleaned_row = [clean_value(val) for val in row[1:]]
                batch.append(cleaned_row)
                if len(batch) >= batch_size:
                    cur.executemany("""
                        INSERT INTO covid_data (
                            city, city_ibge_code, date, epidemiological_week,
                            estimated_population, estimated_population_2019, is_last,
                            is_repeated, last_available_confirmed,
                            last_available_confirmed_per_100k_inhabitants, last_available_date,
                            last_available_death_rate, last_available_deaths,
                            order_for_place, place_type, state, new_confirmed,
                            new_deaths, daily_variation
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch)
                    conn.commit()
                    batch = []
                    logger.info("Batch inserted")

            if batch:
                cur.executemany("""
                    INSERT INTO covid_data (
                        city, city_ibge_code, date, epidemiological_week,
                        estimated_population, estimated_population_2019, is_last,
                        is_repeated, last_available_confirmed,
                        last_available_confirmed_per_100k_inhabitants, last_available_date,
                        last_available_death_rate, last_available_deaths,
                        order_for_place, place_type, state, new_confirmed,
                        new_deaths, daily_variation
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                conn.commit()
                logger.info("Batch inserted")

        cur.close()
        conn.close()
        logger.info("CSV loaded into PostgreSQL successfully")

    except FileNotFoundError as e:
        logger.error("CSV file not found: %s", e)
    except psycopg2.Error as e:
        logger.error("Error loading CSV to PostgreSQL: %s", e)
    except Exception as e:
        logger.error("Unexpected error: %s", e)

if __name__ == "__main__":
    #csv_file_path = '/opt/airflow/data/file.csv'
    csv_file_path = '/opt/airflow/data/file-minimal-data.csv'
    load_csv_to_postgres(csv_file_path)
