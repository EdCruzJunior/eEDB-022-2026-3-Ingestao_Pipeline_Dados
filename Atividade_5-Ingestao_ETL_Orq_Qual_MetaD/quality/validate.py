import json
import os
from pathlib import Path

import psycopg2
import pandas as pd
import great_expectations as gx

OUT = Path("/opt/airflow/data/quality")
OUT.mkdir(parents=True, exist_ok=True)


def validate():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="atividade5",
        user="atividade",
        password="atividade123",
    )
    try:
        df = pd.read_sql_query("SELECT * FROM silver.reclamacoes", conn)
    finally:
        conn.close()

    validator = gx.from_pandas(df)

    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    validator.expect_column_values_to_not_be_null("ano")
    validator.expect_column_values_to_not_be_null("instituicao_financeira")
    validator.expect_column_values_to_be_between("ano", min_value=2021, max_value=2022)
    validator.expect_column_values_to_be_in_set("trimestre", value_set=[1, 2, 3, 4])
    validator.expect_column_values_to_be_between("total_reclamacoes", min_value=0, max_value=None)
    validator.expect_column_values_to_be_between("total_clientes_ccs_scr", min_value=0, max_value=None)

    result = validator.validate()
    result_dict = result.to_json_dict()

    calc = (
        df["reclamacoes_reguladas_procedentes"].fillna(0)
        + df["reclamacoes_reguladas_outras"].fillna(0)
        + df["reclamacoes_nao_reguladas"].fillna(0)
    )
    business_pass = bool((calc == df["total_reclamacoes"].fillna(0)).all())
    business_detail = {
        "expectation": "total_reclamacoes = procedentes + outras + nao_reguladas",
        "success": business_pass,
        "rows_evaluated": int(len(df)),
    }

    result_dict["business_rule"] = business_detail
    (OUT / "great_expectations_result.json").write_text(
        json.dumps(result_dict, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8"
    )

    success = bool(result["success"]) and business_pass
    summary = {
        "success": success,
        "rows_evaluated": int(len(df)),
        "ge_success": bool(result["success"]),
        "business_rule_success": business_pass,
    }
    (OUT / "quality_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not success:
        raise RuntimeError("Validação de qualidade reprovada.")
    return summary


if __name__ == "__main__":
    validate()
