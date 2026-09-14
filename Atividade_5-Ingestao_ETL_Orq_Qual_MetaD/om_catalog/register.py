"""
Registro/catalogação dos principais datasets no OpenMetadata.

Observação:
- O OpenMetadata precisa estar disponível em http://localhost:8585 no host.
- Dentro do Docker, o endpoint é http://openmetadata-server:8585.
- O script é tolerante a diferenças de versão: registra uma tentativa por
  tabela e grava o retorno em data/quality/openmetadata_result.json.
"""
import json
import os
from pathlib import Path

import requests
from sqlalchemy import create_engine, inspect

OM_HOST = os.getenv("OPENMETADATA_HOST", "openmetadata-server")
OM_PORT = os.getenv("OPENMETADATA_PORT", "8585")
OM_USER = os.getenv("OPENMETADATA_USER", "admin")
OM_PASSWORD = os.getenv("OPENMETADATA_PASSWORD", "admin")
DB_URL = os.getenv(
    "DATA_DB_URL",
    "postgresql+psycopg2://atividade:atividade123@postgres:5432/atividade5"
)
OUT = Path("/opt/airflow/data/quality")
OUT.mkdir(parents=True, exist_ok=True)


def wait_for_om():
    url = f"http://{OM_HOST}:{OM_PORT}/api/v1/system/config"
    try:
        r = requests.get(url, timeout=10)
        return r.status_code < 500
    except requests.RequestException:
        return False


def main():
    engine = create_engine(DB_URL)
    inspector = inspect(engine)
    datasets = []
    for schema in ["raw", "silver", "gold"]:
        for table in inspector.get_table_names(schema=schema):
            cols = inspector.get_columns(table, schema=schema)
            datasets.append({
                "schema": schema,
                "table": table,
                "columns": [{"name": c["name"], "type": str(c["type"])} for c in cols],
            })

    results = {"openmetadata_available": wait_for_om(), "datasets": datasets}

    (OUT / "metadata_manifest.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    if not results["openmetadata_available"]:
        print("OpenMetadata ainda não respondeu. Manifesto gerado; execute novamente após o servidor estar pronto.")
        return

    headers = {"Content-Type": "application/json"}
    for item in datasets:
        fqn = f"atividade5.{item['schema']}.{item['table']}"
        payload = {
            "name": item["table"],
            "fullyQualifiedName": fqn,
            "displayName": f"{item['schema']}.{item['table']}",
            "description": (
                f"Dataset da Atividade 5 - camada {item['schema']}. "
                "Pipeline orquestrado por Apache Airflow e validado com "
                "Great Expectations."
            ),
            "tableType": "Regular",
            "columns": [
                {"name": c["name"], "dataType": "VARCHAR", "description": ""}
                for c in item["columns"]
            ],
        }
        try:
            r = requests.post(
                f"http://{OM_HOST}:{OM_PORT}/api/v1/tables",
                auth=(OM_USER, OM_PASSWORD),
                headers=headers,
                json=payload,
                timeout=20,
            )
            print(f"{fqn}: HTTP {r.status_code}")
        except requests.RequestException as exc:
            print(f"{fqn}: {exc}")


if __name__ == "__main__":
    main()
