# ETL com PySpark + Python em Docker com integração AWS S3

## Atividade Acadêmica

**Nome:** Edvaldo da Cruz Junior  
**Curso:** Engenharia de Dados + Big Data  
**Instituição:** Universidade de São Paulo – Escola Politécnica (USP – POLI)

---

## 1. Objetivo

Esta atividade apresenta a implementação de um pipeline de **ETL (Extract, Transform, Load)** utilizando **Python + PySpark**, executado em um ambiente local conteinerizado com **Docker**, com possibilidade de integração com o **Amazon S3**.

O objetivo é demonstrar conceitos de Engenharia de Dados, incluindo processamento com Apache Spark, ETL com PySpark, Docker, leitura de CSV/TSV, tratamento e padronização dos dados, integração de fontes, geração de indicadores, armazenamento em Parquet, particionamento e utilização do Amazon S3 como camada de Data Lake.

A arquitetura permite que o processamento seja realizado localmente, evitando a necessidade de manter recursos computacionais como EC2 ou EMR ativos na AWS durante o estudo.

> **Custos:** a solução foi desenhada para minimizar custos. S3 pode gerar cobranças conforme armazenamento, requisições e condições da conta AWS. Recomenda-se usar arquivos pequenos, monitorar Billing/Free Tier e excluir os recursos ao final.

---

## 2. Arquivos utilizados

### Dados trimestrais

```text
2021_tri_01.csv
2021_tri_02.csv
2021_tri_03.csv
2021_tri_04.csv
2022_tri_01.csv
2022_tri_03.csv
2022_tri_04.csv
```

> O conjunto fornecido não possui `2022_tri_02.csv`; essa ausência deve ser registrada como característica da fonte disponibilizada.

### Enquadramento

```text
EnquadramentoInicia.tsv
```

### Glassdoor

```text
glassdoor_join_match.csv
glassdoor_join_match_less.csv
```

---

## 3. Tecnologias

| Tecnologia | Finalidade |
|---|---|
| Python | Desenvolvimento do pipeline |
| PySpark | ETL e processamento |
| Apache Spark | Motor de processamento |
| Docker | Containerização |
| Docker Compose | Orquestração local |
| Parquet | Armazenamento analítico |
| Amazon S3 | Data Lake / armazenamento Cloud |
| Hadoop S3A | Integração Spark → S3 |
| boto3 | Biblioteca AWS |
| AWS CLI | Administração e testes |

---

## 4. Arquitetura

```text
                     DOCKER
┌──────────────────────────────────────────────┐
│                                              │
│  Spark Master  ────────>  Spark Worker       │
│       │                                      │
│       ▼                                      │
│  Python + PySpark                            │
│       │                                      │
│       ├── EXTRACT                            │
│       ├── TRANSFORM                          │
│       └── LOAD                               │
│              │                               │
│              ▼                               │
│       data/processed/                       │
│              │                               │
└──────────────┼───────────────────────────────┘
               │
             s3a://
               ▼
          Amazon S3
       ┌──────────────┐
       │ raw/         │
       │ processed/   │
       └──────────────┘
```

O `load.py` permite:

```text
LOAD_MODE=LOCAL
        ↓
data/processed/

LOAD_MODE=S3
        ↓
s3a://bucket/processed/
```

---

## 5. Estrutura do projeto

```text
ETL_PySpark_AWS/
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env
├── .gitignore
├── README.md
│
├── data/
│   ├── raw/
│   ├── utf8/
│   └── processed/
│
├── src/
│   ├── prepare_data.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── main.py
│   └── analysis.py
│
└── output/
```

---

## 6. Preparação dos arquivos

Os arquivos originais devem ser colocados em:

```text
data/raw/
```

Recomenda-se remover os sufixos `(5)` e `(6)` dos nomes dos arquivos para facilitar sua utilização nos scripts.

O pipeline trata os diferentes delimitadores:

- arquivos trimestrais: `;`
- enquadramento: TAB
- Glassdoor: `|`

Uma etapa `prepare_data.py` pode normalizar a codificação dos arquivos para UTF-8 antes da leitura pelo Spark.

Fluxo:

```text
data/raw/
    ↓
prepare_data.py
    ↓
data/utf8/
```

---

## 7. Docker

Verificar:

```powershell
docker --version
docker compose version
```

Construir:

```powershell
docker compose build
```

Subir:

```powershell
docker compose up -d
```

Verificar:

```powershell
docker compose ps
```

A interface do Spark Master estará em:

```text
http://localhost:8080
```

O Spark Worker estará em:

```text
http://localhost:8081
```

---

## 8. ETAPA EXTRACT

Os arquivos são carregados como DataFrames Spark.

Exemplo CSV trimestral:

```python
df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", True)
    .csv(path)
)
```

Enquadramento:

```python
df = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .csv(path)
)
```

Glassdoor:

```python
df = (
    spark.read
    .option("header", True)
    .option("sep", "|")
    .csv(path)
)
```

---

## 9. ETAPA TRANSFORM

As transformações incluem:

- remoção de colunas desnecessárias;
- limpeza de espaços;
- padronização de nomes;
- normalização de CNPJ;
- conversão de tipos;
- criação de indicadores;
- integração das fontes;
- preparação para Parquet.

Exemplo de CNPJ:

```python
from pyspark.sql.functions import regexp_replace, trim, col

df = df.withColumn(
    "cnpj_if",
    regexp_replace(
        trim(col("CNPJ IF")),
        "[^0-9]",
        ""
    )
)
```

Indicador de reclamações por 1.000 clientes:

```text
total_reclamacoes / total_clientes × 1000
```

Taxa de procedência:

```text
reclamacoes_procedentes / total_reclamacoes × 100
```

---

## 10. Integração das fontes

A integração proposta é:

```text
RECLAMAÇÕES
      │
      │ CNPJ / Nome
      ▼
ENQUADRAMENTO
      │
      │ CNPJ / Nome
      ▼
GLASSDOOR
```

O CNPJ deve ser normalizado antes do JOIN. Quando não houver CNPJ disponível, pode-se utilizar correspondência complementar por nome, considerando o percentual de correspondência disponível no Glassdoor.

Não se deve assumir que todos os registros terão correspondência.

---

## 11. ETAPA LOAD

O `src/load.py` possui duas modalidades.

### Local

No `.env`:

```dotenv
LOAD_MODE=LOCAL
```

Resultado:

```text
data/processed/reclamacoes/
```

### AWS S3

No `.env`:

```dotenv
LOAD_MODE=S3
AWS_BUCKET=SEU_BUCKET
AWS_DEFAULT_REGION=sa-east-1
```

Resultado:

```text
s3a://SEU_BUCKET/processed/reclamacoes/
```

---

## 12. Particionamento

O dataset de reclamações pode ser particionado por:

```text
Ano
Trimestre
```

Resultado:

```text
processed/
└── reclamacoes/
    ├── Ano=2021/
    │   ├── Trimestre=1º/
    │   ├── Trimestre=2º/
    │   ├── Trimestre=3º/
    │   └── Trimestre=4º/
    └── Ano=2022/
        ├── Trimestre=1º/
        ├── Trimestre=3º/
        └── Trimestre=4º/
```

O formato utilizado é:

```text
Parquet
```

---

## 13. AWS S3

No Console AWS:

1. Acessar Amazon S3.
2. Criar um bucket com nome globalmente único.
3. Selecionar uma região, por exemplo `sa-east-1`.
4. Manter o bloqueio de acesso público.
5. Utilizar as áreas lógicas:

```text
raw/
processed/
```

Exemplo:

```text
ed-cruz-etl-pyspark-2026/
├── raw/
└── processed/
```

---

## 14. IAM

A identidade utilizada pelo laboratório deve possuir somente as permissões necessárias ao bucket, como:

```text
ListBucket
GetObject
PutObject
```

Não é recomendado utilizar permissões administrativas para esta atividade.

---

## 15. Credenciais

As credenciais não devem ser gravadas no código Python.

Arquivo `.env`:

```dotenv
LOAD_MODE=S3

AWS_BUCKET=SEU_BUCKET
AWS_DEFAULT_REGION=sa-east-1

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
```

O `.env` deve ser incluído no `.gitignore`.

O código deve obter as credenciais por variáveis de ambiente.

---

## 16. Configuração do Spark para S3A

O acesso utiliza:

```text
s3a://
```

Exemplo:

```text
s3a://meu-bucket/processed/reclamacoes
```

No `SparkSession`:

```python
.config(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
)
```

Fluxo:

```text
.env
 ↓
Docker
 ↓
Variáveis de ambiente
 ↓
Spark
 ↓
Hadoop S3A
 ↓
Amazon S3
```

---

## 17. Execução do ETL

Subir os containers:

```powershell
docker compose up -d
```

Executar:

```powershell
docker exec -it spark-master `
/opt/spark/bin/spark-submit `
--master spark://spark-master:7077 `
/opt/project/src/main.py
```

Fluxo:

```text
EXTRACT
   ↓
TRANSFORM
   ↓
LOAD
```

---

## 18. Teste LOCAL

No `.env`:

```dotenv
LOAD_MODE=LOCAL
```

Executar:

```powershell
docker compose down
docker compose up -d --build
```

Depois:

```powershell
docker exec -it spark-master `
/opt/spark/bin/spark-submit `
--master spark://spark-master:7077 `
/opt/project/src/main.py
```

Verificar:

```text
data/processed/
```

---

## 19. Teste AWS S3

No `.env`:

```dotenv
LOAD_MODE=S3
AWS_BUCKET=SEU_BUCKET
AWS_DEFAULT_REGION=sa-east-1
```

Subir:

```powershell
docker compose down
docker compose up -d --build
```

Executar:

```powershell
docker exec -it spark-master `
/opt/spark/bin/spark-submit `
--master spark://spark-master:7077 `
/opt/project/src/main.py
```

O resultado será gravado em:

```text
s3a://SEU_BUCKET/processed/reclamacoes/
```

---

## 20. Validação no AWS CLI

```powershell
aws s3 ls s3://SEU_BUCKET/processed/ --recursive
```

Para reclamações:

```powershell
aws s3 ls `
s3://SEU_BUCKET/processed/reclamacoes/ `
--recursive
```

---

## 21. Análise

O `analysis.py` pode ler os Parquets e produzir análises de:

- reclamações por ano;
- reclamações por trimestre;
- reclamações por instituição;
- reclamações por 1.000 clientes;
- taxa de procedência;
- comparação com informações do Glassdoor.

Exemplo:

```python
df = spark.read.parquet(
    "/opt/project/data/processed/reclamacoes"
)
```

---

## 22. Data Lake

A organização utilizada é:

```text
                DATA LAKE

                   S3
                    │
          ┌─────────┴─────────┐
          │                   │
         RAW              PROCESSED
          │                   │
         CSV               Parquet
          │                   │
          └─────── ETL ───────┘
```

**RAW** preserva os arquivos de origem.

**PROCESSED** contém os dados tratados e prontos para análises posteriores.

---

## 23. Estratégia de custos

A arquitetura evita a necessidade de recursos computacionais permanentes na AWS.

Não são necessários para esta demonstração:

```text
EC2
EMR
Glue Jobs
RDS
Redshift
EKS
NAT Gateway
```

O processamento ocorre localmente:

```text
Computador
   ↓
Docker
   ↓
PySpark
```

A AWS pode ser utilizada como armazenamento:

```text
PySpark
   ↓
S3
```

Mesmo em um laboratório acadêmico, deve-se monitorar o Billing e excluir os recursos após a demonstração.

---

## 24. Limpeza

Parar os containers:

```powershell
docker compose down
```

Remover imagens locais do projeto:

```powershell
docker compose down --rmi local
```

Excluir objetos do S3, se não forem mais necessários:

```powershell
aws s3 rm s3://SEU_BUCKET --recursive
```

Depois, se aplicável, excluir o bucket pelo Console AWS.

---

## 25. Fluxo completo

```text
Arquivos de origem
       ↓
data/raw/
       ↓
Preparação / UTF-8
       ↓
data/utf8/
       ↓
EXTRACT
       ↓
PySpark DataFrames
       ↓
TRANSFORM
       ├── Limpeza
       ├── Padronização
       ├── JOIN
       └── Indicadores
       ↓
LOAD
       ↓
Parquet
       ↓
┌───────────────┬───────────────┐
│               │               │
▼               ▼               │
LOCAL           AWS S3          │
│               │               │
▼               ▼               │
processed/      s3a://bucket/   │
```

---

## 26. Resultado da atividade

Ao final, a atividade demonstra uma solução de Engenharia de Dados capaz de:

- executar Apache Spark em Docker;
- utilizar PySpark para processamento;
- consumir múltiplos arquivos;
- tratar diferentes delimitadores;
- normalizar dados;
- executar transformações;
- integrar fontes;
- calcular indicadores;
- gerar arquivos Parquet;
- utilizar particionamento;
- armazenar dados localmente;
- armazenar dados no Amazon S3;
- utilizar `s3a://`;
- utilizar credenciais por variáveis de ambiente;
- separar configuração de código;
- demonstrar conceitos básicos de Data Lake.

---

## 27. Evolução futura

A arquitetura pode ser ampliada posteriormente com:

```text
Airflow
Great Expectations
AWS Lambda
Amazon SQS
DLQ
CloudWatch
AWS Glue
Amazon Athena
```

Esses componentes permitiriam evoluir o laboratório para uma arquitetura mais completa de ingestão, orquestração, qualidade, observabilidade e processamento Cloud.

---

## 28. Comandos rápidos

### Construir

```powershell
docker compose build
```

### Subir

```powershell
docker compose up -d
```

### Ver containers

```powershell
docker compose ps
```

### Executar ETL

```powershell
docker exec -it spark-master `
/opt/spark/bin/spark-submit `
--master spark://spark-master:7077 `
/opt/project/src/main.py
```

### Logs

```powershell
docker compose logs -f spark-master
```

### Parar

```powershell
docker compose down
```

### Ver S3

```powershell
aws s3 ls s3://SEU_BUCKET/processed/ --recursive
```

---

## Conclusão

A atividade implementa um pipeline ETL utilizando **Python, PySpark, Apache Spark e Docker**, com uma camada opcional de armazenamento no **Amazon S3**.

A separação entre **Extract, Transform e Load**, combinada com o armazenamento em **Parquet particionado**, cria uma base adequada para futuras etapas de análise e evolução para uma arquitetura de Data Lake.

A utilização de variáveis de ambiente para credenciais mantém os segredos fora do código-fonte, enquanto a execução local do Spark reduz a necessidade de infraestrutura computacional na AWS durante o estudo.
