import pandas as pd

# Código para enviar relatórios
df = pd.read_csv('/opt/airflow/data/transformed_data.csv')

# Exemplo de envio de relatório
print(df.head())
