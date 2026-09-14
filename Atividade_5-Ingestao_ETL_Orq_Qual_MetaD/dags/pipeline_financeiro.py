from datetime import datetime, timedelta
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
        "owner": "aluno",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }

def run(module):
        subprocess.run([sys.executable, module], check=True)

def ingest():
        run("/opt/airflow/ingestion/ingest.py")

def transform():
        run("/opt/airflow/transformation/transform.py")

def quality():
        run("/opt/airflow/quality/validate.py")

def metadata():
        run("/opt/airflow/om_catalog/register.py")

with DAG(
        dag_id="atividade5_ingestao_financeira",
        default_args=default_args,
        description="Atividade 5 - Orquestração, Qualidade e Metadados",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["usp", "engenharia-de-dados", "atividade5", "airflow", "quality", "metadata"],
    ) as dag:

        ingestao = PythonOperator(
            task_id="01_ingestao_raw",
            python_callable=ingest,
        )

        transformacao = PythonOperator(
            task_id="02_transformacao_silver_gold",
            python_callable=transform,
        )

        qualidade = PythonOperator(
            task_id="03_validacao_great_expectations",
            python_callable=quality,
        )

        catalogo = PythonOperator(
            task_id="04_catalogacao_openmetadata",
            python_callable=metadata,
        )

        ingestao >> transformacao >> qualidade >> catalogo
