import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

base_path = r'C:\Users\Erick Bryan\Documents\GitHub\teste-candidatura-cilia-tecnologia\Dataset-1'

calendar = pd.read_csv(os.path.join(base_path, 'Calendar.csv'), encoding='latin1')
customers = pd.read_csv(os.path.join(base_path, 'Custumers.csv'), encoding='latin1')
product = pd.read_csv(os.path.join(base_path, 'Product.csv'), encoding='latin1')
sales = pd.read_csv(os.path.join(base_path, 'Sales.csv'), encoding='latin1', low_memory=False)
territory = pd.read_csv(os.path.join(base_path, 'Territory.csv'), encoding='latin1')

print("\nColunas de calendar:")
print(calendar.columns)
print("\nColunas de customers:")
print(customers.columns)
print("\nColunas de product:")
print(product.columns)
print("\nColunas de sales antes de quaisquer modificações:")
print(sales.columns)
print("\nColunas de territory:")
print(territory.columns)

engine = create_engine('postgresql+psycopg2://postgres:trojan19@localhost:5432/cilia-tecnologia')
conn = psycopg2.connect(dbname='cilia-tecnologia', user='postgres', password='trojan19', host='localhost', port='5432')
cur = conn.cursor()

print("Remoenção de chaves primárias e estrangeiras...")
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

cur.close()

calendar.to_sql('calendar', engine, if_exists='replace', index=False)
customers.to_sql('custumers', engine, if_exists='replace', index=False)  # Nome da tabela corrigido
product.to_sql('product', engine, if_exists='replace', index=False)
sales.to_sql('sales', engine, if_exists='replace', index=False)

print("\nDados de territory antes de carregar para o banco de dados:")
print(territory.head())

territory.to_sql('territory', engine, if_exists='replace', index=False)

conn = psycopg2.connect(dbname='cilia-tecnologia', user='postgres', password='trojan19', host='localhost', port='5432')
cur = conn.cursor()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
tables = cur.fetchall()
print("\nTabelas no banco de dados:")
for table in tables:
    print(table)

cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='territory';")
territory_schema = cur.fetchall()
print("\nEsquema da tabela territory:")
for column in territory_schema:
    print(column)

primary_keys_queries = [
    """
    ALTER TABLE product
    ADD CONSTRAINT pk_product PRIMARY KEY ("ProductKey");
    """,
    """
    ALTER TABLE custumers
    ADD CONSTRAINT pk_custumers PRIMARY KEY ("CustomerKey");
    """,
    """
    ALTER TABLE territory
    ADD CONSTRAINT pk_territory PRIMARY KEY ("SalesTerritoryKey");
    """
]

for query in primary_keys_queries:
    try:
        cur.execute(query)
        conn.commit()
        print(f"Sucesso: {query.strip()}")
    except Exception as e:
        print(f"Erro ao adicionar chave primária: {e}")

foreign_keys_queries = [
    """
    ALTER TABLE sales
    ADD CONSTRAINT fk_product
    FOREIGN KEY ("ProductKey") REFERENCES product("ProductKey");
    """,
    """
    ALTER TABLE sales
    ADD CONSTRAINT fk_customer
    FOREIGN KEY ("CustomerKey") REFERENCES custumers("CustomerKey");
    """,
    """
    ALTER TABLE sales
    ADD CONSTRAINT fk_territory
    FOREIGN KEY ("SalesTerritoryKey") REFERENCES territory("SalesTerritoryKey");
    """
]

for query in foreign_keys_queries:
    try:
        cur.execute(query)
        conn.commit()
        print(f"Sucesso: {query.strip()}")
    except Exception as e:
        print(f"Erro ao adicionar chave estrangeira: {e}")

cur.execute("SELECT * FROM territory LIMIT 5;")
territory_check = cur.fetchall()
print("\nDados de territory carregados no banco de dados:")
for row in territory_check:
    print(row)
cur.close()
conn.close()
