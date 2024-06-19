import psycopg2
import csv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_csv_to_postgres():
    try:
        # Conecte ao banco de dados
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
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

        # Carregar o arquivo CSV
        with open('/opt/airflow/data/file.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Pular o cabeçalho
            for row in reader:
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
                """, row[1:])  # Ignorar a primeira coluna "Unnamed: 0"
        conn.commit()
        cur.close()
        conn.close()
        logger.info("CSV loaded into PostgreSQL successfully")

    except Exception as e:
        logger.error(f"Error loading CSV to PostgreSQL: {e}")

if __name__ == "__main__":
    load_csv_to_postgres()
