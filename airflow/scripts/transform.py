import pandas as pd

def transform_data():
    # Leia os dados extraídos
    df = pd.read_csv('/opt/airflow/data/extracted_data.csv')

    # Exemplo de transformação: adicionar uma nova coluna
    df['new_column'] = df['last_available_confirmed'].apply(lambda x: x * 2)

    # Salve os dados transformados em um novo arquivo CSV
    df.to_csv('/opt/airflow/data/transformed_data.csv', index=False)

if __name__ == "__main__":
    transform_data()
