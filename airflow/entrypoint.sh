#!/bin/bash

# Instala as dependências
pip install --no-cache-dir -r /requirements/req1.txt
pip install --no-cache-dir -r /requirements/req2.txt
pip install --no-cache-dir -r /requirements/req3.txt

# Inicializa o banco de dados do Airflow
airflow db init

# Cria o usuário admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Executa o comando passado para o contêiner
exec airflow webserver
