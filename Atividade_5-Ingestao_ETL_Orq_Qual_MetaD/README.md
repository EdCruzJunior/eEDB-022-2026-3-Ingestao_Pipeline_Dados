# Atividade 5 – Orquestração, Qualidade e Metadados

## 1. Objetivo

Implementar um pipeline de dados completo utilizando os arquivos disponibilizados nas atividades anteriores, acrescentando:

- **Apache Airflow** para orquestração;
- **Great Expectations** para qualidade de dados;
- **OpenMetadata** para catalogação e metadados;
- **PostgreSQL** para persistência;
- **Python + SQL** para ingestão e transformação;
- **Docker Compose** para reprodução do ambiente.

## 2. Dados utilizados

Foram incorporados ao projeto os 10 arquivos anexados.

### Dados trimestrais de reclamações

| Arquivo | Registros |
|---|---:|
| 2021_tri_01(4).csv | 105 |
| 2021_tri_02(4).csv | 111 |
| 2021_tri_03(4).csv | 113 |
| 2021_tri_04(4).csv | 135 |
| 2022_tri_01(4).csv | 137 |
| 2022_tri_03(4).csv | 163 |
| 2022_tri_04(4).csv | 154 |
| **Total** | **918** |

Os demais arquivos são:

- `EnquadramentoInicia_v2(3).tsv` – enquadramento/segmentação de instituições;
- `glassdoor_consolidado_join_match_v2(3).csv` – dados consolidados do Glassdoor associados às instituições;
- `glassdoor_consolidado_join_match_less_v2(3).csv` – conjunto reduzido para comparação/match.

**Observação:** não foi criado artificialmente o arquivo `2022_tri_02`, pois ele não está entre os arquivos fornecidos.

## 3. Arquitetura

```text
                 CSV / TSV
                    |
                    v
             +-------------+
             |   AIRFLOW   |
             | Orquestração|
             +------+------+
                    |
                    v
             +-------------+
             |   Python    |
             |   Ingestão  |
             +------+------+
                    |
                    v
             +-------------+
             | PostgreSQL  |
             |     RAW     |
             +------+------+
                    |
                    v
             +-------------+
             |   Silver    |
             | Tratamento  |
             +------+------+
                    |
                    v
             +-------------------+
             | Great Expectations|
             |    Qualidade      |
             +---------+---------+
                       |
                       v
             +-------------+
             |    GOLD     |
             | Indicadores |
             +------+------+
                    |
                    v
             +-------------+
             | OpenMetadata|
             |  Catálogo   |
             +-------------+
```

## 4. Tecnologias

- Docker / Docker Compose
- Apache Airflow 2.10.5
- PostgreSQL 16
- Great Expectations 0.18.22
- OpenMetadata 1.6.6
- Python 3.11
- Pandas
- SQLAlchemy

## 5. Estrutura do projeto

```text
atividade5_ingestao_dados/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── README.md
│
├── dags/
│   └── pipeline_financeiro.py
│
├── ingestion/
│   └── ingest.py
│
├── quality/
│   ├── validate.py
│   └── expectations.py
│
├── transformation/
│   └── transform.py
│
├── om_catalog/
│   ├── register.py
│   └── metadata_config.yml
│
├── sql/
│   ├── create_tables.sql
│   ├── silver.sql
│   └── gold.sql
│
├── scripts/
│   ├── init_data_db.sql
│   └── check_results.py
│
├── data/
│   ├── raw/          # arquivos originais anexados
│   ├── processed/
│   └── quality/      # resultados de validação/metadados
│
└── logs/
```

## 6. Camadas de dados

### RAW

Recebe os arquivos sem alterar o conteúdo de negócio, mantendo `arquivo_origem` e `data_ingestao`.

Tabelas:

- `raw.reclamacoes`
- `raw.enquadramento`
- `raw.glassdoor_match`

### SILVER

Realiza:

- padronização de nomes;
- limpeza de strings;
- conversão numérica;
- tratamento de nulos;
- transformação do trimestre;
- eliminação de linhas sem instituição financeira.

Tabela principal:

- `silver.reclamacoes`

### GOLD

Cria indicadores analíticos:

- `gold.indicadores_reclamacoes`
- `gold.ranking_instituicoes`

## 7. Processo de ingestão

O script `ingestion/ingest.py`:

1. Localiza os 7 arquivos trimestrais;
2. Lê CSV usando `;` e `latin1`;
3. Remove a coluna vazia `Unnamed`;
4. Normaliza os nomes das colunas;
5. Converte os indicadores numéricos;
6. Concatena os períodos;
7. Carrega em `raw.reclamacoes`;
8. Lê o TSV de enquadramento;
9. Lê os arquivos Glassdoor usando `|`;
10. Persiste os dados no PostgreSQL.

## 8. Qualidade de dados – Great Expectations

As principais regras são:

| Regra | Critério |
|---|---|
| Volume | tabela Silver deve possuir registros |
| Ano | não nulo e entre 2021 e 2022 |
| Trimestre | 1, 2, 3 ou 4 |
| Instituição | não nula |
| Reclamações | `total_reclamacoes >= 0` |
| Clientes | `total_clientes_ccs_scr >= 0` |
| Regra de negócio | total = procedentes + outras + não reguladas |

O resultado é salvo em:

```text
data/quality/great_expectations_result.json
data/quality/quality_summary.json
```

A DAG interrompe a continuidade caso a validação seja reprovada.

## 9. Orquestração – Airflow

DAG:

```text
01_ingestao_raw
       |
       v
02_transformacao_silver_gold
       |
       v
03_validacao_great_expectations
       |
       v
04_catalogacao_openmetadata
```

A DAG é manual (`schedule=None`) para facilitar a demonstração acadêmica.

## 10. Metadados – OpenMetadata

O processo cria um manifesto com:

- schema;
- tabela;
- colunas;
- tipos;
- camada RAW/SILVER/GOLD;
- origem do dataset;
- ferramentas utilizadas.

O manifesto é:

```text
data/quality/metadata_manifest.json
```

O script `om_catalog/register.py` também tenta registrar os datasets via API do OpenMetadata quando o servidor estiver disponível.

## 11. Data lineage

A linhagem conceitual do projeto é:

```text
2021_tri_01.csv ─┐
2021_tri_02.csv ─┤
2021_tri_03.csv ─┤
2021_tri_04.csv ─┤
2022_tri_01.csv ─┤
2022_tri_03.csv ─┤
2022_tri_04.csv ─┘
                  |
                  v
           raw.reclamacoes
                  |
                  v
          silver.reclamacoes
                  |
                  v
       gold.indicadores_reclamacoes
                  |
                  v
        gold.ranking_instituicoes
```

## 12. Como executar

### Pré-requisitos

Instalar:

- Docker Desktop;
- Docker Compose.

Verificar:

```bash
docker --version
docker compose version
```

### Subir o ambiente

Na pasta do projeto:

```bash
docker compose build
docker compose up -d
```

Verificar:

```bash
docker compose ps
```

## 13. Acessar o Airflow

Abrir:

```text
http://localhost:8084
```

Credenciais:

```text
Usuário: admin
Senha: admin
```

Localizar a DAG:

```text
atividade5_ingestao_financeira
```

Ativar a DAG e executar manualmente.

## 14. Acessar o PostgreSQL

Do computador host:

```text
Host: localhost
Port: 5432
Database: atividade5
User: atividade
Password: atividade123
```

Exemplo:

```bash
docker exec -it atividade5-postgres psql -U atividade -d atividade5
```

Consultar:

```sql
SELECT COUNT(*) FROM raw.reclamacoes;
SELECT COUNT(*) FROM silver.reclamacoes;
SELECT COUNT(*) FROM gold.indicadores_reclamacoes;
SELECT * FROM gold.ranking_instituicoes ORDER BY ranking LIMIT 10;
```

## 15. Acessar o OpenMetadata

Abrir:

```text
http://localhost:8585
```

Usuário inicial:

```text
admin@open-metadata.org
```

Senha:

```text
admin
```

> A primeira inicialização pode levar alguns minutos devido à inicialização do MySQL, Elasticsearch e OpenMetadata.

## 16. Evidências para a apresentação

Recomenda-se capturar:

1. Arquivos originais em `data/raw`;
2. Estrutura do projeto;
3. PostgreSQL com schemas RAW/SILVER/GOLD;
4. DAG do Airflow;
5. Execução da DAG com todas as tarefas em SUCCESS;
6. Resultado do Great Expectations;
7. Exemplo de regra de qualidade reprovada;
8. Dataset catalogado no OpenMetadata;
9. Colunas e descrições;
10. Lineage.

## 17. Demonstração de falha de qualidade

Para demonstrar o funcionamento do Great Expectations, pode-se alterar temporariamente um valor em Silver para:

```text
total_reclamacoes = -10
```

Executar novamente a validação.

Resultado esperado:

```text
03_validacao_great_expectations
FAILED
```

Após restaurar o dado:

```text
SUCCESS
```

Essa evidência demonstra que a qualidade não é apenas documentada, mas efetivamente utilizada como controle do pipeline.

## 18. Critérios da atividade

| Item solicitado | Implementação |
|---|---|
| Ferramenta de orquestração | Apache Airflow |
| Ferramenta de qualidade | Great Expectations |
| Ferramenta de metadados | OpenMetadata |
| Processo de ingestão | Python |
| Persistência | PostgreSQL |
| Transformação | SQL + Python |
| Containerização | Docker Compose |
| Controle de qualidade | Expectations + regra de negócio |
| Metadados | Catálogo + manifesto |
| Linhagem | RAW → SILVER → GOLD |

## 19. Resultado esperado

Ao final da execução, o pipeline deverá produzir:

```text
RAW
 ├── reclamacoes
 ├── enquadramento
 └── glassdoor_match

SILVER
 └── reclamacoes

GOLD
 ├── indicadores_reclamacoes
 └── ranking_instituicoes

QUALITY
 ├── great_expectations_result.json
 └── quality_summary.json

METADATA
 └── metadata_manifest.json
```

## 20. Conclusão

O projeto demonstra a evolução de um processo de ingestão para um pipeline de engenharia de dados com mecanismos de orquestração, qualidade e governança.

O Apache Airflow controla a execução e dependência das etapas. O Great Expectations valida os dados utilizando regras técnicas e de negócio. O OpenMetadata permite catalogar os datasets e documentar seus metadados.

A arquitetura RAW → SILVER → GOLD permite separar dados de origem, dados tratados e informações destinadas à análise, aumentando a organização, rastreabilidade e confiabilidade do processo.

---

**Projeto preparado para execução com Docker Compose e para utilização como base da entrega da Atividade 5.**
