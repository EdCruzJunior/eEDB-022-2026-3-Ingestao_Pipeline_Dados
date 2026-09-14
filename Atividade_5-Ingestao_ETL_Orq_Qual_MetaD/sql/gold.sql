DROP TABLE IF EXISTS gold.indicadores_reclamacoes;

CREATE TABLE gold.indicadores_reclamacoes AS
SELECT
    ano,
    trimestre,
    instituicao_financeira,
    cnpj_if,
    SUM(total_reclamacoes) AS total_reclamacoes,
    SUM(reclamacoes_reguladas_procedentes) AS reclamacoes_procedentes,
    SUM(reclamacoes_reguladas_outras) AS reclamacoes_reguladas_outras,
    SUM(reclamacoes_nao_reguladas) AS reclamacoes_nao_reguladas,
    SUM(total_clientes_ccs_scr) AS total_clientes,
    ROUND(
        SUM(total_reclamacoes)::numeric /
        NULLIF(SUM(total_clientes_ccs_scr), 0) * 10000, 4
    ) AS reclamacoes_por_10_mil_clientes
FROM silver.reclamacoes
GROUP BY ano, trimestre, instituicao_financeira, cnpj_if;

DROP TABLE IF EXISTS gold.ranking_instituicoes;

CREATE TABLE gold.ranking_instituicoes AS
SELECT
    instituicao_financeira,
    SUM(total_reclamacoes) AS total_reclamacoes,
    SUM(total_clientes) AS total_clientes,
    ROUND(
        SUM(total_reclamacoes)::numeric /
        NULLIF(SUM(total_clientes), 0) * 10000, 4
    ) AS reclamacoes_por_10_mil_clientes,
    DENSE_RANK() OVER (
        ORDER BY
            SUM(total_reclamacoes)::numeric /
            NULLIF(SUM(total_clientes), 0) DESC
    ) AS ranking
FROM gold.indicadores_reclamacoes
GROUP BY instituicao_financeira;
