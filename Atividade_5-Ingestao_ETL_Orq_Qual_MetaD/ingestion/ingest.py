
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

BASE = Path("/opt/airflow/data/raw")
SQL_DIR = Path("/opt/airflow/sql")
DB_URL = os.getenv(
        "DATA_DB_URL",
        "postgresql+psycopg2://atividade:atividade123@postgres:5432/atividade5"
    )
engine = create_engine(DB_URL)


def ensure_database_objects():
        sql = (SQL_DIR / "create_tables.sql").read_text(encoding="utf-8")
        with engine.begin() as conn:
            conn.execute(text(sql))
        print("Estruturas de banco validadas/criadas.")


def insert_df(table_name, schema, df):
        if df.empty:
            return
        columns = list(df.columns)
        values = [tuple(None if pd.isna(v) else v for v in row) for row in df[columns].itertuples(index=False, name=None)]
        connection = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="atividade5",
            user="atividade",
            password="atividade123",
        )
        try:
            with connection.cursor() as cur:
                quoted_cols = ", ".join(columns)
                insert_sql = f"INSERT INTO {schema}.{table_name} ({quoted_cols}) VALUES %s"
                execute_values(cur, insert_sql, values)
            connection.commit()
        finally:
            connection.close()
        print(f"Inseridos {len(values)} registros em {schema}.{table_name}.")

QUARTERLY_FILES = sorted(BASE.glob("20*_tri_*.csv"))
ENQUADRAMENTO = BASE / "EnquadramentoInicia_v2(3).tsv"
GLASSDOOR = BASE / "glassdoor_consolidado_join_match_v2(3).csv"
GLASSDOOR_LESS = BASE / "glassdoor_consolidado_join_match_less_v2(3).csv"

def clean_text(value):
        if pd.isna(value):
            return None
        return str(value).strip()

def numeric_series(series):
        return pd.to_numeric(
            series.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
            errors="coerce"
        )


def normalize_columns(cols):
        normalized = []
        for col in cols:
            value = str(col).strip()
            value = unicodedata.normalize("NFKD", value)
            value = value.encode("ascii", "ignore").decode("ascii")
            value = value.lower()
            value = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
            normalized.append(value)
        return normalized


def ingest_quarterly():
        frames = []
        for path in QUARTERLY_FILES:
            df = pd.read_csv(path, sep=";", encoding="latin1")
            df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed")], errors="ignore")
            df.columns = normalize_columns(df.columns)
            rename = {
                "cnpj_if": "cnpj_if",
                "indice": "indice",
                "quantidade_de_reclamacoes_reguladas_procedentes": "reclamacoes_reguladas_procedentes",
                "quantidade_de_reclamacoes_reguladas_outras": "reclamacoes_reguladas_outras",
                "quantidade_de_reclamacoes_nao_reguladas": "reclamacoes_nao_reguladas",
                "quantidade_total_de_reclamacoes": "total_reclamacoes",
                "quantidade_total_de_clientes_ccs_e_scr": "total_clientes_ccs_scr",
                "quantidade_de_clientes_ccs": "clientes_ccs",
                "quantidade_de_clientes_scr": "clientes_scr",
                "instituicao_financeira": "instituicao_financeira",
                "trimestre": "trimestre",
                "ano": "ano",
                "categoria": "categoria",
                "tipo": "tipo",
            }
            df = df.rename(columns=rename)
            for col in [
                "indice", "reclamacoes_reguladas_procedentes",
                "reclamacoes_reguladas_outras", "reclamacoes_nao_reguladas",
                "total_reclamacoes", "total_clientes_ccs_scr",
                "clientes_ccs", "clientes_scr"
            ]:
                if col in df:
                    df[col] = numeric_series(df[col])
            df["arquivo_origem"] = path.name
            frames.append(df)

        if not frames:
            raise RuntimeError("Nenhum arquivo trimestral foi encontrado.")

        final = pd.concat(frames, ignore_index=True)
        cols = [
            "ano", "trimestre", "categoria", "tipo", "cnpj_if",
            "instituicao_financeira", "indice",
            "reclamacoes_reguladas_procedentes",
            "reclamacoes_reguladas_outras",
            "reclamacoes_nao_reguladas", "total_reclamacoes",
            "total_clientes_ccs_scr", "clientes_ccs", "clientes_scr",
            "arquivo_origem"
        ]
        final = final[[c for c in cols if c in final.columns]]
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE raw.reclamacoes"))
        insert_df("reclamacoes", "raw", final)
        print(f"RAW reclamações: {len(final)} registros carregados.")
        return len(final)

def ingest_enquadramento():
        df = pd.read_csv(ENQUADRAMENTO, sep="\t", encoding="utf-8")
        df.columns = ["segmento", "cnpj", "nome"]
        df["cnpj"] = df["cnpj"].astype(str).str.replace(r"\.0$", "", regex=True)
        df["arquivo_origem"] = ENQUADRAMENTO.name
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE raw.enquadramento"))
        insert_df("enquadramento", "raw", df)
        print(f"RAW enquadramento: {len(df)} registros carregados.")


def ingest_glassdoor():
        # O arquivo Glassdoor é separado por pipe (|).
        df = pd.read_csv(GLASSDOOR, sep="|", encoding="utf-8-sig")
        df = df.rename(columns={
            "employer-website": "employer_website",
            "employer-headquarters": "employer_headquarters",
            "employer-founded": "employer_founded",
            "employer-industry": "employer_industry",
            "employer-revenue": "employer_revenue",
            "Geral": "geral",
            "Cultura e valores": "cultura_valores",
            "Diversidade e inclusão": "diversidade_inclusao",
            "Qualidade de vida": "qualidade_vida",
            "Alta liderança": "alta_lideranca",
            "Remuneração e benefícios": "remuneracao_beneficios",
            "Oportunidades de carreira": "oportunidades_carreira",
            "Recomendam para outras pessoas(%)": "recomendam_pct",
            "Perspectiva positiva da empresa(%)": "perspectiva_positiva_pct",
        })
        df["arquivo_origem"] = GLASSDOOR.name
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE raw.glassdoor_match"))
        insert_df("glassdoor_match", "raw", df)
        print(f"RAW Glassdoor: {len(df)} registros carregados.")


if __name__ == "__main__":
        ensure_database_objects()
        ingest_quarterly()
        ingest_enquadramento()
        ingest_glassdoor()
