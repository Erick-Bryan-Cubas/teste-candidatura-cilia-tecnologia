#!/bin/bash

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
