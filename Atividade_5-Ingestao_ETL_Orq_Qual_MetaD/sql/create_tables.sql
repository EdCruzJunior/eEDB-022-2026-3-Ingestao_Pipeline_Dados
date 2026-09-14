CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS raw.reclamacoes (
    ano INTEGER,
    trimestre VARCHAR(10),
    categoria TEXT,
    tipo TEXT,
    cnpj_if VARCHAR(30),
    instituicao_financeira TEXT,
    indice NUMERIC,
    reclamacoes_reguladas_procedentes INTEGER,
    reclamacoes_reguladas_outras INTEGER,
    reclamacoes_nao_reguladas INTEGER,
    total_reclamacoes INTEGER,
    total_clientes_ccs_scr BIGINT,
    clientes_ccs BIGINT,
    clientes_scr BIGINT,
    arquivo_origem TEXT,
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.enquadramento (
    segmento VARCHAR(20),
    cnpj VARCHAR(30),
    nome TEXT,
    arquivo_origem TEXT,
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.glassdoor_match (
    employer_name TEXT,
    reviews_count INTEGER,
    culture_count INTEGER,
    salaries_count INTEGER,
    benefits_count INTEGER,
    employer_website TEXT,
    employer_headquarters TEXT,
    employer_founded INTEGER,
    employer_industry TEXT,
    employer_revenue TEXT,
    url TEXT,
    geral NUMERIC,
    cultura_valores NUMERIC,
    diversidade_inclusao NUMERIC,
    qualidade_vida NUMERIC,
    alta_lideranca NUMERIC,
    remuneracao_beneficios NUMERIC,
    oportunidades_carreira NUMERIC,
    recomendam_pct NUMERIC,
    perspectiva_positiva_pct NUMERIC,
    segmento VARCHAR(20),
    nome TEXT,
    match_percent NUMERIC,
    arquivo_origem TEXT,
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
