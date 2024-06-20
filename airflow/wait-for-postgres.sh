#!/bin/bash

# Espera pelo PostgreSQL estar pronto
until pg_isready -h postgres -U airflow; do
  >&2 echo "Postgres não está pronto - dormindo"
  sleep 1
done

>&2 echo "Postgres está pronto - executando comando"
exec "$@"
