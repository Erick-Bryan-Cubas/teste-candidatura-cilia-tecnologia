import pandas as pd

# Código para transformar os dados
df = pd.read_csv('/opt/airflow/data/extracted_data.csv')

# Exemplo de transformação
df['new_column'] = df['some_column'].apply(lambda x: x * 2)

df.to_csv('/opt/airflow/data/transformed_data.csv', index=False)
