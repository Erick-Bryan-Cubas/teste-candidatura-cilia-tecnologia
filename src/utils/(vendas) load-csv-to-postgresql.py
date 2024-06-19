import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Definindo constantes e caminhos
base_dir = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.join(base_dir, '..','..', 'Dataset-1')
DB_URL = 'postgresql+psycopg2://postgres:trojan19@localhost:5432/cilia-tecnologia'
DB_PARAMS = {
    'dbname': 'cilia-tecnologia',
    'user': 'postgres',
    'password': 'trojan19',
    'host': 'localhost',
    'port': '5432'
}

files = os.listdir(BASE_PATH)
print("Arquivos no diretório Dataset-1:")
print(files)

# Função para carregar os dados
def load_data(file_name):
    return pd.read_csv(os.path.join(BASE_PATH, file_name), encoding='latin1')

# Carregando os dados
calendar = load_data('Calendar.csv')
customers = load_data('Custumers.csv')
product = load_data('Product.csv')
sales = load_data('Sales.csv')
territory = load_data('Territory.csv')

# Exibindo colunas dos datasets
datasets = {'calendar': calendar, 'customers': customers, 'product': product, 'sales': sales, 'territory': territory}
for name, df in datasets.items():
    print(f"\nColunas de {name}:")
    print(df.columns)

# No Console de Depuração execute os comandos abaixo para verificar o tipo de dado de cada coluna
#print(sales['TotalProductCost'].dtype)
#print(sales['TotalProductCost'].head())
# Transformando colunas de string para numérico
columns_to_convert = ['TotalProductCost', 'SalesAmount', 'TaxAmt', 'UnitPrice']
for col in columns_to_convert:
    sales[col] = pd.to_numeric(sales[col].str.replace(',', '.'), errors='coerce')

# Conectando ao banco de dados
engine = create_engine(DB_URL)
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Remoção de chaves primárias e estrangeiras
foreign_keys_removal_queries = [
    "ALTER TABLE sales DROP CONSTRAINT IF EXISTS fk_product;",
    "ALTER TABLE sales DROP CONSTRAINT IF EXISTS fk_customer;",
    "ALTER TABLE custumers DROP CONSTRAINT IF EXISTS fk_territory;"
]

for query in foreign_keys_removal_queries:
    try:
        cur.execute(query)
        conn.commit()
        print(f"Sucesso ao remover: {query.strip()}")
    except Exception as e:
        print(f"Erro ao remover chave estrangeira: {e}")

# Fechando cursor temporariamente
cur.close()

# Salvando dados no banco de dados
calendar.to_sql('calendar', engine, if_exists='replace', index=False)
customers.to_sql('custumers', engine, if_exists='replace', index=False)  # Nome da tabela corrigido
product.to_sql('product', engine, if_exists='replace', index=False)
sales.to_sql('sales', engine, if_exists='replace', index=False)
territory.to_sql('territory', engine, if_exists='replace', index=False)

# Exibindo os dados de territory
print("\nDados de territory antes de carregar para o banco de dados:")
print(territory.head())

# Reabrindo conexão e cursor
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Exibindo tabelas no banco de dados
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
tables = cur.fetchall()
print("\nTabelas no banco de dados:")
for table in tables:
    print(table)

# Exibindo esquema da tabela territory
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='territory';")
territory_schema = cur.fetchall()
print("\nEsquema da tabela territory:")
for column in territory_schema:
    print(column)

# Adicionando chaves primárias
primary_keys_queries = [
    "ALTER TABLE product ADD CONSTRAINT pk_product PRIMARY KEY (\"ProductKey\");",
    "ALTER TABLE custumers ADD CONSTRAINT pk_custumers PRIMARY KEY (\"CustomerKey\");",
    "ALTER TABLE territory ADD CONSTRAINT pk_territory PRIMARY KEY (\"SalesTerritoryKey\");"
]

for query in primary_keys_queries:
    try:
        cur.execute(query)
        conn.commit()
        print(f"Sucesso: {query.strip()}")
    except Exception as e:
        print(f"Erro ao adicionar chave primária: {e}")

# Adicionando chaves estrangeiras
foreign_keys_queries = [
    "ALTER TABLE sales ADD CONSTRAINT fk_product FOREIGN KEY (\"ProductKey\") REFERENCES product(\"ProductKey\");",
    "ALTER TABLE sales ADD CONSTRAINT fk_customer FOREIGN KEY (\"CustomerKey\") REFERENCES custumers(\"CustomerKey\");",
    "ALTER TABLE sales ADD CONSTRAINT fk_territory FOREIGN KEY (\"SalesTerritoryKey\") REFERENCES territory(\"SalesTerritoryKey\");"
]

for query in foreign_keys_queries:
    try:
        cur.execute(query)
        conn.commit()
        print(f"Sucesso: {query.strip()}")
    except Exception as e:
        print(f"Erro ao adicionar chave estrangeira: {e}")

# Exibindo dados da tabela territory
cur.execute("SELECT * FROM territory LIMIT 5;")
territory_check = cur.fetchall()
print("\nDados de territory carregados no banco de dados:")
for row in territory_check:
    print(row)

# Fechando conexão
cur.close()
conn.close()
