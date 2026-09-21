import csv
import os
import re
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "streaming")
PG_USER = os.getenv("PG_USER", "streaming")
PG_PASSWORD = os.getenv("PG_PASSWORD", "streaming")

def norm_digits(v):
    return re.sub(r"\D", "", str(v or ""))

def norm_name(v):
    v = re.sub(r"\s*\(.*\)$", "", str(v or ""))
    v = re.sub(r"\s+", " ", v).strip().upper()
    return v

conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS institution_enrichment (
        cnpj_norm TEXT PRIMARY KEY,
        segmento TEXT,
        nome TEXT
    );
    CREATE TABLE IF NOT EXISTS glassdoor_enrichment (
        nome_norm TEXT PRIMARY KEY,
        geral DOUBLE PRECISION,
        cultura_valores DOUBLE PRECISION,
        diversidade_inclusao DOUBLE PRECISION,
        qualidade_vida DOUBLE PRECISION,
        alta_lideranca DOUBLE PRECISION,
        remuneracao_beneficios DOUBLE PRECISION,
        oportunidades_carreira DOUBLE PRECISION,
        recomendam_pct DOUBLE PRECISION,
        perspectiva_positiva_pct DOUBLE PRECISION
    );
    """)

    tsv = DATA_DIR / "EnquadramentoInicia_v2(6).tsv"
    rows_by_cnpj = {}
    with tsv.open(encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            cnpj = norm_digits(r["CNPJ"])
            if cnpj:
                rows_by_cnpj[cnpj] = (cnpj, r["Segmento"], r["Nome"])
    rows = list(rows_by_cnpj.values())
    execute_values(cur, """
        INSERT INTO institution_enrichment (cnpj_norm, segmento, nome)
        VALUES %s
        ON CONFLICT (cnpj_norm) DO UPDATE
        SET segmento=EXCLUDED.segmento, nome=EXCLUDED.nome
    """, rows)

    gd = DATA_DIR / "glassdoor_consolidado_join_match_v2(6).csv"
    gd_rows_by_nome = {}
    with gd.open(encoding="latin1", newline="") as f:
        for r in csv.DictReader(f, delimiter="|"):
            def num(k):
                try: return float(str(r.get(k, "")).replace(",", "."))
                except: return None
            nome_norm = norm_name(r.get("Nome") or r.get("employer_name"))
            if not nome_norm:
                continue
            gd_rows_by_nome[nome_norm] = (
                nome_norm,
                num("Geral"), num("Cultura e valores"), num("Diversidade e inclusão"),
                num("Qualidade de vida"), num("Alta liderança"), num("Remuneração e benefícios"),
                num("Oportunidades de carreira"), num("Recomendam para outras pessoas(%)"),
                num("Perspectiva positiva da empresa(%)")
            )
    gd_rows = list(gd_rows_by_nome.values())
    execute_values(cur, """
        INSERT INTO glassdoor_enrichment
        (nome_norm, geral, cultura_valores, diversidade_inclusao, qualidade_vida,
         alta_lideranca, remuneracao_beneficios, oportunidades_carreira,
         recomendam_pct, perspectiva_positiva_pct)
        VALUES %s
        ON CONFLICT (nome_norm) DO UPDATE SET
          geral=EXCLUDED.geral,
          cultura_valores=EXCLUDED.cultura_valores,
          diversidade_inclusao=EXCLUDED.diversidade_inclusao,
          qualidade_vida=EXCLUDED.qualidade_vida,
          alta_lideranca=EXCLUDED.alta_lideranca,
          remuneracao_beneficios=EXCLUDED.remuneracao_beneficios,
          oportunidades_carreira=EXCLUDED.oportunidades_carreira,
          recomendam_pct=EXCLUDED.recomendam_pct,
          perspectiva_positiva_pct=EXCLUDED.perspectiva_positiva_pct
    """, gd_rows)

    print("institution_enrichment:", len(rows), "registros")
    print("glassdoor_enrichment:", len(gd_rows), "registros")

conn.close()
