# ETL e Ingestão de Dados com Python — Reclamações do Sistema Financeiro + Enquadramento + Glassdoor

## 1. Objetivo

Este projeto implementa uma solução de **ingestão, tratamento, integração e disponibilização de dados utilizando Python**, atendendo ao enunciado acadêmico:

- Utilizar linguagem de programação Python para ingestão e tratamento;
- Utilizar pacotes adicionais, exceto **Apache Spark** e **DuckDB**;
- Não realizar o tratamento de dados via SQL;
- Ingerir todas as bases em um banco de dados relacional open source;
- Gerar uma tabela final com os dados tratados e unidos;
- Implementar as camadas **RAW**, **TRUSTED** e **DELIVERY**;
- Armazenar Trusted e Delivery em **Parquet**;
- Disponibilizar a camada Delivery também como uma **tabela final dentro do banco relacional**.

### Tecnologias

- Python 3.11
- Pandas
- PyArrow
- SQLAlchemy
- Psycopg2
- python-dotenv
- openpyxl
- PostgreSQL
- Docker

### Tecnologias não utilizadas

- Apache Spark: **não utilizado**
- DuckDB: **não utilizado**

---

# 2. Fontes de dados utilizadas

As bases reais fornecidas para este projeto são utilizadas como fontes de entrada da pipeline.

## 2.1 Bases trimestrais de reclamações

Foram fornecidas sete bases trimestrais:

| Arquivo | Ano | Trimestre | Registros |
|---|---:|---:|---:|
| `2021_tri_01.csv` | 2021 | 1º | 105 |
| `2021_tri_02.csv` | 2021 | 2º | 111 |
| `2021_tri_03.csv` | 2021 | 3º | 113 |
| `2021_tri_04.csv` | 2021 | 4º | 135 |
| `2022_tri_01.csv` | 2022 | 1º | 137 |
| `2022_tri_03.csv` | 2022 | 3º | 163 |
| `2022_tri_04.csv` | 2022 | 4º | 154 |
| **Total** | | | **918** |

> **Observação:** não foi fornecido o arquivo `2022_tri_02`. Portanto, a pipeline utiliza somente os períodos efetivamente disponibilizados.

As bases trimestrais possuem a mesma estrutura principal, com informações como:

```text
Ano
Trimestre
Categoria
Tipo
CNPJ IF
Instituição financeira
Índice
Quantidade de reclamações reguladas procedentes
Quantidade de reclamações reguladas - outras
Quantidade de reclamações não reguladas
Quantidade total de reclamações
Quantidade total de clientes - CCS e SCR
Quantidade de clientes - CCS
Quantidade de clientes - SCR
```

Os arquivos foram identificados como arquivos separados por `;` e com codificação compatível com `latin1`.

---

## 2.2 Base de enquadramento

Arquivo:

```text
EnquadramentoInicia_v2.tsv
```

Formato:

```text
TSV
```

Separador:

```text
\t
```

Colunas:

```text
Segmento
CNPJ
Nome
```

Quantidade de registros:

```text
1.474
```

Esta base será utilizada para complementar as informações das instituições financeiras, principalmente através da chave:

```text
CNPJ
```

---

## 2.3 Bases do Glassdoor

Foram fornecidas duas bases relacionadas ao Glassdoor:

### Base 1

```text
glassdoor_consolidado_join_match_v2.csv
```

Registros:

```text
34
```

Principais campos:

```text
employer_name
reviews_count
culture_count
salaries_count
benefits_count
employer-website
employer-headquarters
employer-founded
employer-industry
employer-revenue
url
Geral
Cultura e valores
Diversidade e inclusão
Qualidade de vida
Alta liderança
Remuneração e benefícios
Oportunidades de carreira
Recomendam para outras pessoas(%)
Perspectiva positiva da empresa(%)
Segmento
Nome
match_percent
```

Esta base contém informações de avaliação das empresas no Glassdoor e o respectivo relacionamento com o enquadramento.

---

### Base 2

```text
glassdoor_consolidado_join_match_less_v2.csv
```

Registros:

```text
5
```

Principais campos:

```text
employer_name
reviews_count
culture_count
salaries_count
benefits_count
employer-website
employer-headquarters
employer-founded
employer-industry
employer-revenue
url
Geral
Cultura e valores
Diversidade e inclusão
Qualidade de vida
Alta liderança
Remuneração e benefícios
Oportunidades de carreira
Recomendam para outras pessoas(%)
Perspectiva positiva da empresa(%)
CNPJ
Nome
match_percent
```

Esta base possui o CNPJ explicitamente, permitindo relacionamento direto com as bases de reclamações.

---

# 3. Inventário das fontes

```text
data/input/
|
+-- 2021_tri_01.csv
+-- 2021_tri_02.csv
+-- 2021_tri_03.csv
+-- 2021_tri_04.csv
|
+-- 2022_tri_01.csv
+-- 2022_tri_03.csv
+-- 2022_tri_04.csv
|
+-- EnquadramentoInicia_v2.tsv
|
+-- glassdoor_consolidado_join_match_v2.csv
+-- glassdoor_consolidado_join_match_less_v2.csv
```

---

# 4. Arquitetura da solução

```text
                       FONTES
                         |
        +----------------+----------------+
        |                |                |
        v                v                v
  Reclamações       Enquadramento     Glassdoor
  trimestrais           TSV             CSV
        |                |                |
        +----------------+----------------+
                         |
                         v
                  Python / Pandas
                         |
                         v
              +---------------------+
              |        RAW          |
              |      PostgreSQL     |
              |                     |
              | Dados originais    |
              +----------+----------+
                         |
                         v
                  Python / Pandas
                         |
              +----------+----------+
              |                     |
              v                     v
        Normalização            Validação
        Tipagem                 Qualidade
        Limpeza                 Duplicidades
              |                     |
              +----------+----------+
                         |
                         v
              +---------------------+
              |      TRUSTED        |
              |       Parquet       |
              | Dados tratados      |
              +----------+----------+
                         |
                         v
                  Python / Pandas
                         |
                 JOIN / MERGE
                         |
             +-----------+-----------+
             |                       |
             v                       v
       CNPJ + Segmento          Glassdoor
             |                       |
             +-----------+-----------+
                         |
                         v
              +---------------------+
              |      DELIVERY       |
              |       Parquet       |
              | Dataset final       |
              +----------+----------+
                         |
                         v
              +---------------------+
              |     PostgreSQL      |
              |                     |
              | delivery.           |
              | reclamacoes_final   |
              +---------------------+
```

---

# 5. Estrutura do projeto

```text
projeto_etl_python/
|
+-- data/
|   |
|   +-- input/
|   |   +-- 2021_tri_01.csv
|   |   +-- 2021_tri_02.csv
|   |   +-- 2021_tri_03.csv
|   |   +-- 2021_tri_04.csv
|   |   +-- 2022_tri_01.csv
|   |   +-- 2022_tri_03.csv
|   |   +-- 2022_tri_04.csv
|   |   +-- EnquadramentoInicia_v2.tsv
|   |   +-- glassdoor_consolidado_join_match_v2.csv
|   |   +-- glassdoor_consolidado_join_match_less_v2.csv
|   |
|   +-- trusted/
|   |   +-- reclamacoes.parquet
|   |   +-- enquadramento.parquet
|   |   +-- glassdoor_match.parquet
|   |   +-- glassdoor_match_less.parquet
|   |
|   +-- delivery/
|       +-- reclamacoes_glassdoor_final.parquet
|
+-- src/
|   +-- 01_ingestao_raw.py
|   +-- 02_trusted.py
|   +-- 03_delivery.py
|   +-- 04_carga_delivery.py
|   +-- 05_validacao.py
|   +-- config.py
|
+-- sql/
|   +-- 01_criar_schemas.sql
|
+-- .env
+-- .gitignore
+-- requirements.txt
+-- docker-compose.yml
+-- run_pipeline.py
+-- README.md
```

---

# 6. Pré-requisitos

Instalar:

1. Docker Desktop
2. Python 3.11
3. Visual Studio Code
4. Git, se o projeto for versionado

Verificar:

```bash
python --version
```

Recomendado:

```text
Python 3.11.x
```

---

# 7. Criar PostgreSQL com Docker

Criar o arquivo `docker-compose.yml`:

```yaml
services:

  postgres:
    image: postgres:15
    container_name: postgres-etl
    restart: unless-stopped

    environment:
      POSTGRES_DB: etl_python
      POSTGRES_USER: etl_user
      POSTGRES_PASSWORD: etl_password

    ports:
      - "5432:5432"

    volumes:
      - postgres_etl_data:/var/lib/postgresql/data

volumes:
  postgres_etl_data:
```

Subir:

```bash
docker compose up -d
```

Verificar:

```bash
docker ps
```

Esperado:

```text
postgres-etl
```

---

# 8. Criar ambiente virtual

Windows:

```bash
python -m venv .venv  #py -3.11 -m venv .venv ( para criação de um ambiente com o Python 3.11 )
```

Ativar:

```bash
.venv\Scripts\activate
```

Atualizar pip:

```bash
python -m pip install --upgrade pip
```

---

# 9. Instalar dependências

Criar `requirements.txt`:

```text
pandas==2.2.3
pyarrow==18.1.0
SQLAlchemy==2.0.36
psycopg2-binary==2.9.10
python-dotenv==1.0.1
openpyxl==3.1.5
```

Instalar:

```bash
pip install -r requirements.txt
```

---

# 10. Configurar conexão PostgreSQL

Criar `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_python
DB_USER=etl_user
DB_PASSWORD=etl_password
```

Não versionar o `.env`.

Adicionar ao `.gitignore`:

```text
.env
.venv/
__pycache__/
*.pyc
```

---

# 11. Configuração Python

Criar `src/config.py`:

```python
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{DB_USER}:{DB_PASSWORD}@"
    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


engine = create_engine(DATABASE_URL)
```

---

# 12. Criar schemas

O SQL é utilizado apenas para criação da infraestrutura.

Não são utilizadas regras de tratamento ou JOIN em SQL.

Criar:

```text
sql/01_criar_schemas.sql
```

Conteúdo:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS delivery;
```

Executar:

```bash
psql -h localhost -U etl_user -d etl_python -f sql/01_criar_schemas.sql
```
Comando para PowerShell:
```PowerShell
Get-Content sql/01_criar_schemas.sql | docker exec -i postgres-etl psql -U etl_user -d etl_python
```

---

# 13. Preparar as fontes

Copiar todos os arquivos fornecidos para:

```text
data/input/
```

A estrutura deve ser:

```text
data/input/
|
+-- 2021_tri_01.csv
+-- 2021_tri_02.csv
+-- 2021_tri_03.csv
+-- 2021_tri_04.csv
+-- 2022_tri_01.csv
+-- 2022_tri_03.csv
+-- 2022_tri_04.csv
+-- EnquadramentoInicia_v2.tsv
+-- glassdoor_consolidado_join_match_v2.csv
+-- glassdoor_consolidado_join_match_less_v2.csv
```

---

# 14. Características das fontes

## 14.1 Reclamações trimestrais

Os arquivos trimestrais são:

```text
CSV
Separador: ;
Encoding: latin1
```

O tratamento de encoding é importante porque os arquivos possuem caracteres acentuados.

Exemplo:

```python
pd.read_csv(
    arquivo,
    sep=";",
    encoding="latin1",
    dtype=str
)
```

Todos os campos inicialmente são lidos como `str` para evitar perda de zeros à esquerda e permitir que o tratamento de tipos seja controlado pelo Python.

---

## 14.2 Enquadramento

Arquivo:

```text
EnquadramentoInicia_v2(1).tsv
```

Características:

```text
Formato: TSV
Separador: \t
Encoding: latin1
```

Leitura:

```python
pd.read_csv(
    arquivo,
    sep="\t",
    encoding="latin1",
    dtype=str
)
```

---

## 14.3 Glassdoor

Os arquivos Glassdoor utilizam:

```text
Formato: CSV
Separador: |
Encoding: latin1
```

Leitura:

```python
pd.read_csv(
    arquivo,
    sep="|",
    encoding="latin1",
    dtype=str
)
```

---

# 15. Camada RAW

A camada RAW deve representar os dados recebidos das fontes.

Para este projeto, as fontes serão armazenadas em PostgreSQL no schema:

```text
raw
```

Estrutura sugerida:

```text
raw/
|
+-- reclamacoes_2021_tri_01
+-- reclamacoes_2021_tri_02
+-- reclamacoes_2021_tri_03
+-- reclamacoes_2021_tri_04
+-- reclamacoes_2022_tri_01
+-- reclamacoes_2022_tri_03
+-- reclamacoes_2022_tri_04
|
+-- enquadramento
|
+-- glassdoor_match
+-- glassdoor_match_less
```

A RAW preserva a origem e facilita rastreabilidade.

---

# 16. Ingestão RAW

Criar:

```text
src/01_ingestao_raw.py
```

```python
from pathlib import Path

import pandas as pd

from config import engine


INPUT_DIR = Path("data/input")


def ler_arquivo(arquivo):

    nome = arquivo.name.lower()

    if arquivo.suffix.lower() == ".tsv":

        return pd.read_csv(
            arquivo,
            sep="\t",
            encoding="latin1",
            dtype=str
        )

    if "glassdoor" in nome:

        return pd.read_csv(
            arquivo,
            sep="|",
            encoding="latin1",
            dtype=str
        )

    if arquivo.suffix.lower() == ".csv":

        return pd.read_csv(
            arquivo,
            sep=";",
            encoding="latin1",
            dtype=str
        )

    raise ValueError(
        f"Formato não suportado: {arquivo}"
    )


def nome_tabela(nome_arquivo):

    nome = Path(nome_arquivo).stem

    nome = (
        nome
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
    )

    return nome


def ingestao():

    arquivos = sorted(
        INPUT_DIR.iterdir()
    )

    if not arquivos:

        raise FileNotFoundError(
            "Nenhum arquivo encontrado em data/input."
        )

    for arquivo in arquivos:

        if arquivo.suffix.lower() not in [
            ".csv",
            ".tsv"
        ]:
            continue

        print("=" * 70)
        print(f"Arquivo: {arquivo.name}")

        df = ler_arquivo(arquivo)

        tabela = nome_tabela(
            arquivo.name
        )

        print(
            f"Registros: {len(df)}"
        )

        print(
            f"Colunas: {len(df.columns)}"
        )

        df.to_sql(
            name=tabela,
            con=engine,
            schema="raw",
            if_exists="replace",
            index=False
        )

        print(
            f"Tabela criada: raw.{tabela}"
        )


if __name__ == "__main__":

    ingestao()
```

Executar:

```bash
python src/01_ingestao_raw.py
```

---

# 17. Camada TRUSTED

Na Trusted, os dados passam pelos tratamentos utilizando Python/Pandas.

A pipeline realizará:

1. União dos arquivos trimestrais;
2. Padronização das colunas;
3. Remoção da coluna `Unnamed: 14`, quando existente;
4. Normalização do CNPJ;
5. Conversão de campos numéricos;
6. Conversão do índice;
7. Tratamento de nulos;
8. Remoção de duplicidades;
9. Padronização de nomes;
10. Geração de arquivos Parquet.

---

# 18. União das bases trimestrais

As sete bases possuem a mesma estrutura principal.

A união será realizada em Python com:

```python
pd.concat()
```

Exemplo:

```python
df_reclamacoes = pd.concat(
    lista_dataframes,
    ignore_index=True
)
```

Não será utilizado:

```sql
UNION
```

ou qualquer outro mecanismo SQL para a transformação.

---

# 19. Tratamento do CNPJ

Nas bases trimestrais, o campo:

```text
CNPJ IF
```

possui CNPJ sem pontuação e também pode estar vazio em registros de conglomerados.

Na base de enquadramento:

```text
CNPJ
```

é utilizado para relacionamento.

Função:

```python
import re


def normalizar_cnpj(valor):

    if pd.isna(valor):

        return None

    valor = re.sub(
        r"\D",
        "",
        str(valor)
    )

    if not valor:

        return None

    return valor.zfill(14)
```

Caso a origem contenha CNPJ com 8 dígitos para identificação da instituição, o projeto deve preservar a chave de origem separadamente ou aplicar a regra de negócio definida para relacionamento.

Uma alternativa segura é manter:

```text
CNPJ_IF_ORIGINAL
CNPJ_IF_NORMALIZADO
```

evitando perda da informação original.

---

# 20. Tratamento dos campos numéricos

Os arquivos possuem valores decimais utilizando vírgula, por exemplo:

```text
54,79
59,49
82,54
```

Portanto, a conversão deve ser feita em Python:

```python
def converter_decimal(serie):

    return pd.to_numeric(
        serie
        .astype(str)
        .str.replace(
            ",",
            ".",
            regex=False
        ),
        errors="coerce"
    )
```

Para campos quantitativos:

```python
df[coluna] = pd.to_numeric(
    df[coluna],
    errors="coerce"
)
```

---

# 21. Script Trusted

Criar:

```text
src/02_trusted.py
```

```python
from pathlib import Path
import re

import pandas as pd

from config import engine


INPUT_DIR = Path("data/input")
TRUSTED_DIR = Path("data/trusted")

TRUSTED_DIR.mkdir(
    parents=True,
    exist_ok=True
)


ARQUIVOS_RECLAMACOES = [
    "2021_tri_01.csv",
    "2021_tri_02.csv",
    "2021_tri_03.csv",
    "2021_tri_04.csv",
    "2022_tri_01.csv",
    "2022_tri_03.csv",
    "2022_tri_04.csv",
]


COLUNAS_RENOMEAR = {
    "Ano": "ano",
    "Trimestre": "trimestre",
    "Categoria": "categoria",
    "Tipo": "tipo",
    "CNPJ IF": "cnpj_if",
    "Instituição financeira": "instituicao_financeira",
    "Índice": "indice",
    "Quantidade de reclamações reguladas procedentes":
        "qtd_reclamacoes_reguladas_procedentes",
    "Quantidade de reclamações reguladas - outras":
        "qtd_reclamacoes_reguladas_outras",
    "Quantidade de reclamações não reguladas":
        "qtd_reclamacoes_nao_reguladas",
    "Quantidade total de reclamações":
        "qtd_total_reclamacoes",
    "Quantidade total de clientes  CCS e SCR":
        "qtd_clientes_ccs_scr",
    "Quantidade de clientes  CCS":
        "qtd_clientes_ccs",
    "Quantidade de clientes  SCR":
        "qtd_clientes_scr",
}


def normalizar_cnpj(valor):

    if pd.isna(valor):

        return None

    valor = re.sub(
        r"\D",
        "",
        str(valor)
    )

    if not valor:

        return None

    return valor


def normalizar_texto(valor):

    if pd.isna(valor):

        return None

    valor = str(valor).strip()

    if not valor:

        return None

    return valor


def converter_decimal(serie):

    return pd.to_numeric(
        serie
        .astype(str)
        .str.replace(
            ",",
            ".",
            regex=False
        ),
        errors="coerce"
    )


def carregar_reclamacoes():

    lista = []

    for nome in ARQUIVOS_RECLAMACOES:

        arquivo = INPUT_DIR / nome

        print(
            f"Lendo {arquivo.name}"
        )

        df = pd.read_csv(
            arquivo,
            sep=";",
            encoding="latin1",
            dtype=str
        )

        # Remove coluna vazia existente nos arquivos
        df = df.loc[
            :,
            ~df.columns.str.startswith(
                "Unnamed:"
            )
        ]

        lista.append(df)

    df = pd.concat(
        lista,
        ignore_index=True
    )

    return df


def tratar_reclamacoes():

    df = carregar_reclamacoes()

    df = df.rename(
        columns=COLUNAS_RENOMEAR
    )

    # Textos
    colunas_texto = [
        "trimestre",
        "categoria",
        "tipo",
        "instituicao_financeira",
    ]

    for coluna in colunas_texto:

        if coluna in df.columns:

            df[coluna] = (
                df[coluna]
                .apply(normalizar_texto)
            )

    # CNPJ
    df["cnpj_if_original"] = df[
        "cnpj_if"
    ]

    df["cnpj_if"] = df[
        "cnpj_if"
    ].apply(normalizar_cnpj)

    # Numéricos
    colunas_numericas = [
        "ano",
        "qtd_reclamacoes_reguladas_procedentes",
        "qtd_reclamacoes_reguladas_outras",
        "qtd_reclamacoes_nao_reguladas",
        "qtd_total_reclamacoes",
        "qtd_clientes_ccs_scr",
        "qtd_clientes_ccs",
        "qtd_clientes_scr",
    ]

    for coluna in colunas_numericas:

        if coluna in df.columns:

            df[coluna] = pd.to_numeric(
                df[coluna],
                errors="coerce"
            )

    # Índice decimal
    if "indice" in df.columns:

        df["indice"] = converter_decimal(
            df["indice"]
        )

    # Remove duplicidades
    df = df.drop_duplicates()

    # Salva Trusted
    arquivo = (
        TRUSTED_DIR /
        "reclamacoes.parquet"
    )

    df.to_parquet(
        arquivo,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted reclamações: {arquivo}"
    )

    return df


def tratar_enquadramento():

    arquivo = (
        INPUT_DIR /
        "EnquadramentoInicia_v2(1).tsv"
    )

    df = pd.read_csv(
        arquivo,
        sep="\t",
        encoding="latin1",
        dtype=str
    )

    df.columns = [
        "segmento",
        "cnpj",
        "nome"
    ]

    df["cnpj_original"] = df["cnpj"]

    df["cnpj"] = df[
        "cnpj"
    ].apply(normalizar_cnpj)

    df["segmento"] = df[
        "segmento"
    ].apply(normalizar_texto)

    df["nome"] = df[
        "nome"
    ].apply(normalizar_texto)

    df = df.drop_duplicates()

    arquivo_saida = (
        TRUSTED_DIR /
        "enquadramento.parquet"
    )

    df.to_parquet(
        arquivo_saida,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted enquadramento: {arquivo_saida}"
    )

    return df


def tratar_glassdoor():

    arquivo = (
        INPUT_DIR /
        "glassdoor_consolidado_join_match_v2(1).csv"
    )

    df = pd.read_csv(
        arquivo,
        sep="|",
        encoding="latin1",
        dtype=str
    )

    df.columns = [
        str(col).strip().lower()
        for col in df.columns
    ]

    df = df.drop_duplicates()

    arquivo_saida = (
        TRUSTED_DIR /
        "glassdoor_match.parquet"
    )

    df.to_parquet(
        arquivo_saida,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted Glassdoor: {arquivo_saida}"
    )

    return df


def tratar_glassdoor_less():

    arquivo = (
        INPUT_DIR /
        "glassdoor_consolidado_join_match_less_v2(1).csv"
    )

    df = pd.read_csv(
        arquivo,
        sep="|",
        encoding="latin1",
        dtype=str
    )

    df.columns = [
        str(col).strip().lower()
        for col in df.columns
    ]

    df = df.drop_duplicates()

    arquivo_saida = (
        TRUSTED_DIR /
        "glassdoor_match_less.parquet"
    )

    df.to_parquet(
        arquivo_saida,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted Glassdoor less: {arquivo_saida}"
    )

    return df


if __name__ == "__main__":

    tratar_reclamacoes()

    tratar_enquadramento()

    tratar_glassdoor()

    tratar_glassdoor_less()
```

Executar:

```bash
python src/02_trusted.py
```

---

# 22. Arquivos Trusted gerados

Após a execução:

```text
data/trusted/
|
+-- reclamacoes.parquet
+-- enquadramento.parquet
+-- glassdoor_match.parquet
+-- glassdoor_match_less.parquet
```

---

# 23. Estratégia de integração

A integração será realizada exclusivamente em Python.

A primeira relação será:

```text
RECLAMAÇÕES
     |
     | CNPJ
     v
ENQUADRAMENTO
```

Depois, será incorporada a informação do Glassdoor.

O relacionamento deverá respeitar as chaves disponíveis em cada fonte.

### Relação principal

```text
reclamacoes.cnpj_if
       =
enquadramento.cnpj
```

### Glassdoor com CNPJ

A base:

```text
glassdoor_consolidado_join_match_less_v2(1).csv
```

possui:

```text
CNPJ
Nome
match_percent
```

e pode ser relacionada diretamente por CNPJ quando a chave estiver disponível.

### Glassdoor com Segmento/Nome

A base:

```text
glassdoor_consolidado_join_match_v2(1).csv
```

já possui:

```text
Segmento
Nome
match_percent
```

e deve ser tratada como uma fonte de relacionamento baseada no resultado de matching previamente disponibilizado.

> As duas bases Glassdoor são mantidas separadamente na Trusted para evitar duplicidade ou combinação arbitrária. A regra de negócio deve definir se ambas serão concatenadas, se uma será priorizada ou se serão utilizadas para validação do matching.

---

# 24. Script Delivery

Criar:

```text
src/03_delivery.py
```

Exemplo de implementação usando a base Glassdoor com CNPJ como relacionamento:

```python
from pathlib import Path

import pandas as pd


TRUSTED_DIR = Path("data/trusted")
DELIVERY_DIR = Path("data/delivery")

DELIVERY_DIR.mkdir(
    parents=True,
    exist_ok=True
)


def criar_delivery():

    reclamacoes = pd.read_parquet(
        TRUSTED_DIR /
        "reclamacoes.parquet"
    )

    enquadramento = pd.read_parquet(
        TRUSTED_DIR /
        "enquadramento.parquet"
    )

    glassdoor = pd.read_parquet(
        TRUSTED_DIR /
        "glassdoor_match_less.parquet"
    )

    # ==================================================
    # JOIN 1 - RECLAMAÇÕES + ENQUADRAMENTO
    # ==================================================

    df = reclamacoes.merge(
        enquadramento,
        how="left",
        left_on="cnpj_if",
        right_on="cnpj",
        suffixes=(
            "",
            "_enquadramento"
        )
    )

    # ==================================================
    # JOIN 2 - DELIVERY + GLASSDOOR
    # ==================================================

    # A base Glassdoor less possui CNPJ.
    df = df.merge(
        glassdoor,
        how="left",
        left_on="cnpj_if",
        right_on="cnpj",
        suffixes=(
            "",
            "_glassdoor"
        )
    )

    # ==================================================
    # CONTROLES
    # ==================================================

    df["data_processamento"] = (
        pd.Timestamp.now()
    )

    df = df.drop_duplicates()

    # ==================================================
    # DELIVERY PARQUET
    # ==================================================

    arquivo = (
        DELIVERY_DIR /
        "reclamacoes_glassdoor_final.parquet"
    )

    df.to_parquet(
        arquivo,
        engine="pyarrow",
        index=False
    )

    print(
        f"Delivery criada: {arquivo}"
    )

    print(
        f"Total de registros: {len(df)}"
    )

    print(
        f"Total de colunas: {len(df.columns)}"
    )

    return df


if __name__ == "__main__":

    criar_delivery()
```

---

# 25. Alternativa: relacionamento utilizando Segmento e Nome

Caso a regra definida para o projeto seja utilizar a base Glassdoor que contém:

```text
Segmento
Nome
match_percent
```

o relacionamento deve ser feito com os campos normalizados.

Exemplo:

```python
df["nome_join"] = (
    df["nome"]
    .astype(str)
    .str.strip()
    .str.upper()
)

glassdoor["nome_join"] = (
    glassdoor["nome"]
    .astype(str)
    .str.strip()
    .str.upper()
)

df = df.merge(
    glassdoor,
    how="left",
    on="nome_join"
)
```

O campo:

```text
match_percent
```

deve ser preservado para demonstrar a qualidade do matching.

---

# 26. Por que o JOIN é realizado em Python?

O requisito determina:

> O tratamento de dados não deve ser realizado via SQL.

Portanto, o relacionamento entre as bases será realizado utilizando:

```python
pandas.merge()
```

Exemplo:

```python
df_final = df1.merge(
    df2,
    how="left",
    on="cnpj"
)
```

Não serão utilizados:

```sql
JOIN
UNION
UPDATE
CASE
CAST
CONVERT
```

para executar as regras de transformação.

---

# 27. Carga da Delivery no PostgreSQL

A camada Delivery deve existir em dois formatos:

```text
1. Parquet
2. Tabela PostgreSQL
```

Criar:

```text
src/04_carga_delivery.py
```

```python
from pathlib import Path

import pandas as pd

from config import engine


DELIVERY_FILE = (
    Path("data/delivery") /
    "reclamacoes_glassdoor_final.parquet"
)


def carregar_delivery():

    df = pd.read_parquet(
        DELIVERY_FILE
    )

    print(
        f"Registros: {len(df)}"
    )

    df.to_sql(
        name="reclamacoes_glassdoor_final",
        con=engine,
        schema="delivery",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method="multi"
    )

    print(
        "Tabela delivery.reclamacoes_glassdoor_final "
        "carregada com sucesso."
    )


if __name__ == "__main__":

    carregar_delivery()
```

Executar:

```bash
python src/04_carga_delivery.py
```

---

# 28. Resultado final

## PostgreSQL

```text
PostgreSQL
|
+-- raw
|   |
|   +-- 2021_tri_01_2
|   +-- 2021_tri_02_2
|   +-- 2021_tri_03_2
|   +-- 2021_tri_04_2
|   +-- 2022_tri_01_2
|   +-- 2022_tri_03_2
|   +-- 2022_tri_04_2
|   +-- enquadramentoinicia_v2_1
|   +-- glassdoor_consolidado_join_match_v2_1
|   +-- glassdoor_consolidado_join_match_less_v2_1
|
+-- delivery
    |
    +-- reclamacoes_glassdoor_final
```

## Arquivos

```text
data/
|
+-- trusted/
|   +-- reclamacoes.parquet
|   +-- enquadramento.parquet
|   +-- glassdoor_match.parquet
|   +-- glassdoor_match_less.parquet
|
+-- delivery/
    +-- reclamacoes_glassdoor_final.parquet
```

---

# 29. Validação

Criar:

```text
src/05_validacao.py
```

```python
from pathlib import Path

import pandas as pd

from config import engine


def validar():

    print()
    print("=" * 70)
    print("VALIDAÇÃO DO PIPELINE")
    print("=" * 70)

    arquivos_trusted = [
        "reclamacoes.parquet",
        "enquadramento.parquet",
        "glassdoor_match.parquet",
        "glassdoor_match_less.parquet",
    ]

    for nome in arquivos_trusted:

        arquivo = (
            Path("data/trusted") /
            nome
        )

        df = pd.read_parquet(
            arquivo
        )

        print(
            f"Trusted {nome}: "
            f"{len(df)} registros"
        )

    arquivo_delivery = (
        Path("data/delivery") /
        "reclamacoes_glassdoor_final.parquet"
    )

    delivery_parquet = pd.read_parquet(
        arquivo_delivery
    )

    delivery_db = pd.read_sql(
        """
        SELECT *
        FROM delivery.reclamacoes_glassdoor_final
        """,
        engine
    )

    print()
    print(
        "Delivery Parquet:",
        len(delivery_parquet)
    )

    print(
        "Delivery PostgreSQL:",
        len(delivery_db)
    )

    print(
        "Colunas Delivery:",
        len(delivery_db.columns)
    )

    if len(delivery_parquet) == len(delivery_db):

        print(
            "OK - quantidade de registros "
            "consistente."
        )

    else:

        print(
            "ATENÇÃO - quantidade de registros "
            "divergente."
        )


if __name__ == "__main__":

    validar()
```

Executar:

```bash
python src/05_validacao.py
```

---

# 30. Pipeline completo

Criar:

```text
run_pipeline.py
```

```python
import subprocess
import sys


scripts = [
    "src/01_ingestao_raw.py",
    "src/02_trusted.py",
    "src/03_delivery.py",
    "src/04_carga_delivery.py",
]


def executar():

    for script in scripts:

        print()
        print("=" * 70)
        print(f"Executando: {script}")
        print("=" * 70)

        resultado = subprocess.run(
            [
                sys.executable,
                script
            ]
        )

        if resultado.returncode != 0:

            print(
                f"ERRO na execução de {script}"
            )

            sys.exit(
                resultado.returncode
            )

    print()
    print("=" * 70)
    print("PIPELINE EXECUTADO COM SUCESSO")
    print("=" * 70)


if __name__ == "__main__":

    executar()
```

Executar:

```bash
python run_pipeline.py
```

Depois:

```bash
python src/05_validacao.py
```

---

# 31. Fluxo completo de execução

```text
1. Subir PostgreSQL
       |
       v
2. Criar schemas
       |
       v
3. Copiar fontes para data/input
       |
       v
4. Ingestão RAW
       |
       v
5. União das bases trimestrais
       |
       v
6. Tratamento Trusted
       |
       v
7. Geração dos Parquets
       |
       v
8. JOIN Reclamações + Enquadramento
       |
       v
9. JOIN com Glassdoor
       |
       v
10. Geração Delivery Parquet
       |
       v
11. Carga Delivery PostgreSQL
       |
       v
12. Validação
```

---

# 32. Comandos principais

Subir banco:

```bash
docker compose up -d
```

Ativar ambiente:

```bash
.venv\Scripts\activate
```

Instalar dependências:

```bash
pip install -r requirements.txt
```

Executar pipeline:

```bash
python run_pipeline.py
```

Validar:

```bash
python src/05_validacao.py
```

---

# 33. Regras de tratamento

## CNPJ

```text
Origem
    |
    v
Remover caracteres não numéricos
    |
    v
Normalizar representação
```

## Índice

Os valores utilizam vírgula decimal:

```text
54,79
59,49
82,54
```

São convertidos para:

```text
54.79
59.49
82.54
```

através do Python.

## Campos quantitativos

São convertidos utilizando:

```python
pd.to_numeric(
    coluna,
    errors="coerce"
)
```

## Texto

É realizada:

```text
strip()
```

e, quando necessário:

```text
upper()
```

## Duplicidades

```python
df.drop_duplicates()
```

---

# 34. Observação sobre registros sem CNPJ

Nas bases trimestrais existem registros de instituições classificadas como conglomerados em que o campo:

```text
CNPJ IF
```

pode estar vazio.

Esses registros não devem ser eliminados automaticamente.

Eles devem ser preservados na Trusted e na Delivery.

Como o JOIN por CNPJ não encontra correspondência nesses casos, o relacionamento deverá resultar em campos complementares nulos, mantendo o registro original de reclamações.

Isso é importante para não perder informações da fonte.

---

# 35. Observação sobre o arquivo 2022_tri_02

Entre as bases trimestrais disponibilizadas não existe:

```text
2022_tri_02.csv
```

Portanto, o pipeline não deve criar ou inventar esse período.

O conjunto utilizado é:

```text
2021 T1
2021 T2
2021 T3
2021 T4
2022 T1
2022 T3
2022 T4
```

Total observado nas fontes:

```text
918 registros
```

antes dos tratamentos, deduplicações e JOINs.

---

# 36. Controle de qualidade

Recomenda-se validar:

### Quantidade de registros

```python
len(df)
```

### Quantidade de CNPJs

```python
df["cnpj_if"].nunique()
```

### Nulos

```python
df.isna().sum()
```

### Duplicidades

```python
df.duplicated().sum()
```

### Correspondências no JOIN

```python
df["segmento"].notna().sum()
```

### Taxa de correspondência

```python
taxa = (
    df["segmento"].notna().mean()
    * 100
)
```

---

# 37. Justificativa das camadas

## RAW

Objetivo:

- Preservar a origem;
- Facilitar auditoria;
- Permitir reprocessamento;
- Manter rastreabilidade.

Formato:

```text
PostgreSQL
```

## TRUSTED

Objetivo:

- Dados limpos;
- Dados padronizados;
- Tipos tratados;
- Duplicidades tratadas;
- CNPJ normalizado;
- Dados prontos para integração.

Formato:

```text
Parquet
```

## DELIVERY

Objetivo:

- Dataset final;
- Bases integradas;
- Dados prontos para análise;
- Disponibilização para consumo.

Formatos:

```text
Parquet
+
PostgreSQL
```

---

# 38. Atendimento ao enunciado

| Requisito | Implementação |
|---|---|
| Linguagem Python | OK |
| Ingestão com Python | Pandas |
| Tratamento com Python | Pandas |
| Pacotes adicionais | Pandas, PyArrow, SQLAlchemy etc. |
| Spark | Não utilizado |
| DuckDB | Não utilizado |
| Banco relacional open source | PostgreSQL |
| Bases trimestrais | 7 arquivos |
| Base de enquadramento | TSV |
| Bases Glassdoor | 2 arquivos |
| RAW | PostgreSQL |
| Trusted | Parquet |
| Delivery | Parquet |
| Delivery no banco | PostgreSQL |
| JOIN | `pandas.merge()` |
| Tratamento via SQL | Não realizado |
| Tabela final | `delivery.reclamacoes_glassdoor_final` |
| Validação | Python/Pandas |

---

# 39. Texto para documentação

## Ingestão e ETL utilizando Python

Foi desenvolvida uma solução de ingestão e tratamento de dados utilizando a linguagem Python, com as bibliotecas Pandas e PyArrow, sem utilização das tecnologias Apache Spark ou DuckDB.

Como fontes de dados foram utilizadas sete bases trimestrais de reclamações do sistema financeiro referentes aos anos de 2021 e 2022, uma base de enquadramento das instituições financeiras e duas bases consolidadas provenientes do Glassdoor.

As bases trimestrais foram disponibilizadas em arquivos CSV, utilizando separador ponto e vírgula e codificação compatível com Latin-1. A base de enquadramento foi disponibilizada em formato TSV, enquanto as bases do Glassdoor foram disponibilizadas em arquivos CSV utilizando o caractere `|` como separador.

Inicialmente, todos os arquivos foram ingeridos utilizando Python e armazenados na camada RAW de um banco de dados PostgreSQL, preservando os dados recebidos das fontes.

Na camada TRUSTED, os dados foram processados utilizando exclusivamente Python/Pandas. Foram realizadas operações de limpeza, padronização dos nomes das colunas, tratamento de valores nulos, conversão de tipos, tratamento de valores decimais, normalização de CNPJ, remoção de duplicidades e união das bases trimestrais.

Após o tratamento, os dados foram armazenados em formato Parquet utilizando a biblioteca PyArrow.

Na etapa de integração, os dados da camada TRUSTED foram carregados em DataFrames Pandas. O relacionamento entre as bases de reclamações e enquadramento foi realizado através da função `merge()` do Pandas, utilizando o CNPJ como chave de relacionamento quando disponível. As informações provenientes do Glassdoor também foram integradas de acordo com as chaves disponibilizadas pelas respectivas bases.

Nenhuma regra de transformação ou JOIN foi executada através de SQL. O SQL foi utilizado apenas para a criação da infraestrutura do banco e para leitura/carga dos dados.

Ao final, foi criada a camada DELIVERY, contendo o conjunto de dados tratado e integrado em formato Parquet. A mesma camada também foi disponibilizada como tabela final no PostgreSQL, denominada `delivery.reclamacoes_glassdoor_final`.

A arquitetura final contempla as camadas RAW, TRUSTED e DELIVERY, utilizando PostgreSQL para persistência relacional, Parquet para armazenamento intermediário e Python/Pandas para os processos de ingestão, tratamento e integração.

---

# 40. Checklist de entrega

- [ ] Docker instalado
- [ ] PostgreSQL funcionando
- [ ] Python 3.11 instalado
- [ ] Ambiente virtual criado
- [ ] `requirements.txt` instalado
- [ ] `.env` configurado
- [ ] Todas as 10 fontes copiadas para `data/input`
- [ ] Schema `raw` criado
- [ ] Schema `delivery` criado
- [ ] Arquivos trimestrais ingeridos
- [ ] Enquadramento ingerido
- [ ] Glassdoor ingerido
- [ ] Trusted gerada em Parquet
- [ ] CNPJ tratado
- [ ] Valores decimais tratados
- [ ] Duplicidades tratadas
- [ ] JOIN realizado em Python
- [ ] Delivery gerada em Parquet
- [ ] Delivery carregada no PostgreSQL
- [ ] Tabela final criada
- [ ] Validação executada
- [ ] `.env` fora do Git
- [ ] README.md atualizado

---

# 41. Conclusão

A solução atende ao enunciado utilizando Python como elemento central da pipeline.

A arquitetura implementada é:

```text
FONTES
  |
  v
RAW - PostgreSQL
  |
  v
TRUSTED - Parquet
  |
  v
JOIN/TRATAMENTO - Python/Pandas
  |
  v
DELIVERY - Parquet
  |
  v
DELIVERY - PostgreSQL
```

As fontes efetivamente utilizadas são as bases trimestrais de reclamações, a base de enquadramento e as duas bases consolidadas do Glassdoor fornecidas para o projeto.

O processo de tratamento e integração é executado em Python, sem Apache Spark e sem DuckDB, e a camada Delivery é disponibilizada tanto em Parquet quanto em uma tabela final dentro do PostgreSQL.
