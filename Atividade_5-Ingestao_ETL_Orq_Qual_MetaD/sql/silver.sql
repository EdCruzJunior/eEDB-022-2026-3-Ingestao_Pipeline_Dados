DROP TABLE IF EXISTS silver.reclamacoes;

CREATE TABLE silver.reclamacoes AS
SELECT
    ano,
    CASE
        WHEN trimestre LIKE '1%' THEN 1
        WHEN trimestre LIKE '2%' THEN 2
        WHEN trimestre LIKE '3%' THEN 3
        WHEN trimestre LIKE '4%' THEN 4
        ELSE NULL
    END AS trimestre,
    TRIM(categoria) AS categoria,
    TRIM(tipo) AS tipo,
    NULLIF(TRIM(cnpj_if), '') AS cnpj_if,
    TRIM(instituicao_financeira) AS instituicao_financeira,
    indice,
    COALESCE(reclamacoes_reguladas_procedentes, 0) AS reclamacoes_reguladas_procedentes,
    COALESCE(reclamacoes_reguladas_outras, 0) AS reclamacoes_reguladas_outras,
    COALESCE(reclamacoes_nao_reguladas, 0) AS reclamacoes_nao_reguladas,
    COALESCE(total_reclamacoes, 0) AS total_reclamacoes,
    COALESCE(total_clientes_ccs_scr, 0) AS total_clientes_ccs_scr,
    COALESCE(clientes_ccs, 0) AS clientes_ccs,
    COALESCE(clientes_scr, 0) AS clientes_scr,
    arquivo_origem,
    data_ingestao
FROM raw.reclamacoes
WHERE ano IS NOT NULL
  AND instituicao_financeira IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_silver_reclamacoes_periodo
    ON silver.reclamacoes (ano, trimestre);

CREATE INDEX IF NOT EXISTS idx_silver_reclamacoes_cnpj
    ON silver.reclamacoes (cnpj_if);
