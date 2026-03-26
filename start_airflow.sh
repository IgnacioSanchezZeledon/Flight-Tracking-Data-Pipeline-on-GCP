#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

echo "📁 PROJECT_ROOT=$PROJECT_ROOT"

source "$PROJECT_ROOT/.venv/bin/activate"

export AIRFLOW_HOME="$PROJECT_ROOT/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export PYTHONPATH="$PROJECT_ROOT"

echo "✅ AIRFLOW_HOME=$AIRFLOW_HOME"
echo "✅ DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"
echo "✅ DB=PostgreSQL"

airflow db migrate

echo "🚀 Iniciando Airflow en http://localhost:8080 ..."
airflow scheduler &
airflow webserver --port 8080
