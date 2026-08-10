# ETL e Ingestão de Dados com Python, PostgreSQL e Parquet

## 1. Objetivo

Este projeto implementa uma solução de ingestão e ETL utilizando **Python**, atendendo aos seguintes requisitos:

- Utilizar linguagem Python para ingestão e tratamento de dados;
- Utilizar pacotes adicionais, exceto **Apache Spark** e **DuckDB**;
- Não realizar o tratamento dos dados por SQL;
- Realizar a ingestão de todas as bases em um banco de dados relacional open source;
- Gerar uma tabela final com os dados tratados e unidos;
- Implementar as camadas **RAW**, **Trusted** e **Delivery**;
- Armazenar Trusted e Delivery em formato **Parquet**;
- Disponibilizar obrigatoriamente a camada Delivery também como uma tabela final no banco relacional.

A solução utiliza:

- Python 3.11
- Pandas
- PyArrow
- SQLAlchemy
- Psycopg2
- python-dotenv
- openpyxl
- PostgreSQL

**Spark e DuckDB não são utilizados.**

---

# 2. Arquitetura

```text
                    FONTES DE DADOS
              CSV / XLSX / JSON / etc.
                         |
                         v
              +---------------------+
              |        RAW          |
              |    PostgreSQL       |
              | Dados originais     |
              +----------+----------+
                         |
                    Python/Pandas
                         |
                         v
              +---------------------+
              |      TRUSTED        |
              |       Parquet       |
              | Dados tratados      |
              +----------+----------+
                         |
                    Python/Pandas
                  JOIN + tratamento
                         |
                         v
              +---------------------+
              |      DELIVERY       |
              |       Parquet       |
              | Dados finais        |
              +----------+----------+
                         |
                    Python/Pandas
                         |
                         v
              +---------------------+
              |      PostgreSQL     |
              | delivery.           |
              | reclamacoes_final   |
              +---------------------+
```

Fluxo resumido:

```text
Fontes
  |
  v
Python/Pandas
  |
  v
RAW - PostgreSQL
  |
  v
Python/Pandas - tratamento
  |
  v
TRUSTED - Parquet
  |
  v
Python/Pandas - JOIN
  |
  v
DELIVERY - Parquet
  |
  v
PostgreSQL - tabela final
```

---

# 3. Tecnologias

| Tecnologia | Utilização |
|---|---|
| Python 3.11 | Linguagem principal |
| Pandas | Ingestão e tratamento |
| PyArrow | Geração e leitura de Parquet |
| SQLAlchemy | Conexão com PostgreSQL |
| Psycopg2 | Driver PostgreSQL |
| python-dotenv | Variáveis de ambiente |
| openpyxl | Leitura de Excel |
| PostgreSQL | Banco relacional open source |
| Apache Spark | Não utilizado |
| DuckDB | Não utilizado |

---

# 4. Estrutura do projeto

```text
projeto_etl_python/
|
+-- data/
|   +-- input/
|   |   +-- base_clientes.csv
|   |   +-- base_reclamacoes.csv
|   |
|   +-- trusted/
|   |   +-- base_clientes.parquet
|   |   +-- base_reclamacoes.parquet
|   |
|   +-- delivery/
|       +-- reclamacoes_final.parquet
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
+-- requirements.txt
+-- docker-compose.yml
+-- run_pipeline.py
+-- README.md
```

---

# 5. Pré-requisitos

Instalar:

1. Docker Desktop
2. Python 3.11
3. Visual Studio Code (opcional)
4. Git (opcional)

Verificar Python:

```bash
python --version
```

Recomendado:

```text
Python 3.11.x
```

---

# 6. Criar o PostgreSQL com Docker

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

Subir o PostgreSQL:

```bash
docker compose up -d
```

Verificar:

```bash
docker ps
```

O container deverá aparecer como:

```text
postgres-etl
```

---

# 7. Criar ambiente virtual Python

No Windows:

```bash
python -m venv .venv
```

Ativar:

```bash
.venv\Scripts\activate
```

Atualizar o pip:

```bash
python -m pip install --upgrade pip
```

---

# 8. Instalar os pacotes

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

Validar:

```bash
pip list
```

---

# 9. Configuração do banco

Criar o arquivo `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_python
DB_USER=etl_user
DB_PASSWORD=etl_password
```

**Importante:** não versionar o `.env` no Git. Adicionar ao `.gitignore`:

```text
.env
.venv/
__pycache__/
*.pyc
```

---

# 10. Arquivo de configuração Python

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

# 11. Criar os schemas do PostgreSQL

O SQL será utilizado somente para criar a infraestrutura do banco.

**Não serão utilizadas instruções SQL para tratamento ou JOIN dos dados.**

Criar `sql/01_criar_schemas.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS delivery;
```

Executar pelo `psql`:

```bash
psql -h localhost -U etl_user -d etl_python -f sql/01_criar_schemas.sql
```

Também é possível executar pelo pgAdmin.

---

# 12. Preparar os arquivos de entrada

Colocar as bases em:

```text
data/input/
```

Exemplo:

```text
data/input/base_clientes.csv
data/input/base_reclamacoes.csv
```

Exemplo de `base_clientes.csv`:

```text
CNPJ,Nome,UF,Cidade
12.345.678/0001-90,Empresa A,SP,Sao Paulo
98.765.432/0001-10,Empresa B,RJ,Rio de Janeiro
```

Exemplo de `base_reclamacoes.csv`:

```text
CNPJ,Nome,Data_Reclamacao,Tipo_Reclamacao,Descricao
12345678000190,Empresa A,01/07/2026,Produto,Problema no produto
98765432000110,Empresa B,02/07/2026,Servico,Atraso no atendimento
```

Os arquivos podem ser substituídos pelas bases reais do projeto.

---

# 13. Camada RAW

## Objetivo

A camada RAW mantém os dados próximos ao formato original.

Nesta etapa:

- Os arquivos são lidos pelo Python;
- Os dados são carregados no PostgreSQL;
- Não são realizados JOINs;
- Não são realizadas regras de negócio;
- Não são realizadas transformações de tratamento.

A RAW serve como camada de preservação e rastreabilidade da origem.

---

# 14. Script de ingestão RAW

Criar `src/01_ingestao_raw.py`:

```python
from pathlib import Path
import pandas as pd

from config import engine


INPUT_DIR = Path("data/input")


def ler_arquivo(arquivo):

    extensao = arquivo.suffix.lower()

    if extensao == ".csv":

        return pd.read_csv(
            arquivo,
            sep=",",
            encoding="utf-8",
            dtype=str
        )

    elif extensao in [".xlsx", ".xls"]:

        return pd.read_excel(
            arquivo,
            dtype=str
        )

    else:

        raise ValueError(
            f"Formato não suportado: {arquivo}"
        )


def gerar_nome_tabela(nome_arquivo):

    nome = Path(nome_arquivo).stem

    nome = (
        nome
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
    )

    return nome


def ingestao():

    arquivos = list(INPUT_DIR.glob("*"))

    if not arquivos:
        raise FileNotFoundError(
            "Nenhum arquivo encontrado em data/input"
        )

    for arquivo in arquivos:

        if arquivo.suffix.lower() not in [
            ".csv",
            ".xlsx",
            ".xls"
        ]:
            continue

        print(f"Lendo: {arquivo}")

        df = ler_arquivo(arquivo)

        tabela = gerar_nome_tabela(arquivo.name)

        print(
            f"Quantidade de registros: {len(df)}"
        )

        df.to_sql(
            name=tabela,
            con=engine,
            schema="raw",
            if_exists="replace",
            index=False
        )

        print(
            f"RAW criada: raw.{tabela}"
        )


if __name__ == "__main__":
    ingestao()
```

Executar:

```bash
python src/01_ingestao_raw.py
```

Resultado:

```text
PostgreSQL

raw.base_clientes
raw.base_reclamacoes
```

---

# 15. Camada TRUSTED

A camada Trusted é responsável pelo tratamento dos dados.

Os tratamentos são executados exclusivamente utilizando Python/Pandas.

Exemplos:

- Padronização dos nomes das colunas;
- Remoção de espaços;
- Conversão de tipos;
- Tratamento de nulos;
- Normalização de CNPJ;
- Padronização de nomes;
- Tratamento de datas;
- Remoção de duplicidades;
- Validação dos dados.

A saída será armazenada em Parquet.

---

# 16. Script Trusted

Criar `src/02_trusted.py`:

```python
from pathlib import Path
import pandas as pd
import re

from config import engine


TRUSTED_DIR = Path("data/trusted")

TRUSTED_DIR.mkdir(
    parents=True,
    exist_ok=True
)


def normalizar_colunas(df):

    df.columns = [
        normalizar_texto(col)
        for col in df.columns
    ]

    return df


def normalizar_texto(valor):

    if pd.isna(valor):
        return ""

    valor = str(valor)

    valor = valor.strip()

    valor = valor.upper()

    return valor


def normalizar_cnpj(valor):

    if pd.isna(valor):
        return None

    valor = re.sub(
        r"\D",
        "",
        str(valor)
    )

    if valor == "":
        return None

    return valor.zfill(14)


def normalizar_nome(valor):

    if pd.isna(valor):
        return None

    valor = str(valor)

    valor = (
        valor
        .strip()
        .upper()
    )

    valor = re.sub(
        r"\s+",
        " ",
        valor
    )

    return valor


def tratar_clientes():

    print("Lendo RAW de clientes...")

    df = pd.read_sql(
        """
        SELECT *
        FROM raw.base_clientes
        """,
        engine
    )

    df = normalizar_colunas(df)

    if "CNPJ" in df.columns:
        df["CNPJ"] = df["CNPJ"].apply(
            normalizar_cnpj
        )

    if "NOME" in df.columns:
        df["NOME"] = df["NOME"].apply(
            normalizar_nome
        )

    if "UF" in df.columns:
        df["UF"] = df["UF"].apply(
            normalizar_texto
        )

    if "CIDADE" in df.columns:
        df["CIDADE"] = df["CIDADE"].apply(
            normalizar_texto
        )

    df = df.drop_duplicates()

    arquivo = (
        TRUSTED_DIR /
        "base_clientes.parquet"
    )

    df.to_parquet(
        arquivo,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted criada: {arquivo}"
    )

    return df


def tratar_reclamacoes():

    print(
        "Lendo RAW de reclamações..."
    )

    df = pd.read_sql(
        """
        SELECT *
        FROM raw.base_reclamacoes
        """,
        engine
    )

    df = normalizar_colunas(df)

    if "CNPJ" in df.columns:

        df["CNPJ"] = df["CNPJ"].apply(
            normalizar_cnpj
        )

    if "NOME" in df.columns:

        df["NOME"] = df["NOME"].apply(
            normalizar_nome
        )

    if "DATA_RECLAMACAO" in df.columns:

        df["DATA_RECLAMACAO"] = pd.to_datetime(
            df["DATA_RECLAMACAO"],
            errors="coerce",
            dayfirst=True
        )

    if "TIPO_RECLAMACAO" in df.columns:

        df["TIPO_RECLAMACAO"] = (
            df["TIPO_RECLAMACAO"]
            .apply(normalizar_texto)
        )

    if "DESCRICAO" in df.columns:

        df["DESCRICAO"] = (
            df["DESCRICAO"]
            .apply(normalizar_texto)
        )

    df = df.drop_duplicates()

    arquivo = (
        TRUSTED_DIR /
        "base_reclamacoes.parquet"
    )

    df.to_parquet(
        arquivo,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted criada: {arquivo}"
    )

    return df


if __name__ == "__main__":

    tratar_clientes()

    tratar_reclamacoes()
```

Executar:

```bash
python src/02_trusted.py
```

Resultado:

```text
data/
|
+-- trusted/
    +-- base_clientes.parquet
    +-- base_reclamacoes.parquet
```

---

# 17. Observação sobre SQL

No script Trusted existe:

```python
pd.read_sql(
    """
    SELECT *
    FROM raw.base_clientes
    """,
    engine
)
```

Esse SQL é utilizado exclusivamente para **leitura dos dados da camada RAW**.

Não existe tratamento dentro do SQL.

O tratamento é realizado em Python:

```python
df = normalizar_colunas(df)

df["CNPJ"] = df["CNPJ"].apply(
    normalizar_cnpj
)

df["NOME"] = df["NOME"].apply(
    normalizar_nome
)

df = df.drop_duplicates()
```

Portanto:

```text
SQL -> somente leitura da RAW
Python -> tratamento dos dados
```

---

# 18. Camada DELIVERY

A camada Delivery contém o conjunto final de dados tratados e unidos.

Processo:

```text
Trusted Parquet
       |
       v
Pandas
       |
       v
JOIN / merge()
       |
       v
Tratamentos finais
       |
       v
Delivery Parquet
```

---

# 19. JOIN em Python

O JOIN será realizado através do Pandas:

```python
df_final = reclamacoes.merge(
    clientes,
    how="left",
    on="CNPJ",
    suffixes=(
        "_RECLAMACAO",
        "_CLIENTE"
    )
)
```

Não será utilizado SQL para o JOIN.

---

# 20. JOIN por CNPJ e Nome

Caso o relacionamento do projeto utilize CNPJ e Nome como chaves:

```python
df_final = reclamacoes.merge(
    clientes,
    how="left",
    on=[
        "CNPJ",
        "NOME"
    ]
)
```

Caso o CNPJ seja a chave principal, recomenda-se:

```python
df_final = reclamacoes.merge(
    clientes,
    how="left",
    on="CNPJ"
)
```

O Nome pode ser utilizado como validação.

---

# 21. Script Delivery

Criar `src/03_delivery.py`:

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

    print("Lendo Trusted...")

    clientes = pd.read_parquet(
        TRUSTED_DIR /
        "base_clientes.parquet"
    )

    reclamacoes = pd.read_parquet(
        TRUSTED_DIR /
        "base_reclamacoes.parquet"
    )

    print(
        f"Clientes: {len(clientes)}"
    )

    print(
        f"Reclamações: {len(reclamacoes)}"
    )

    # JOIN realizado exclusivamente em Python
    df_final = reclamacoes.merge(
        clientes,
        how="left",
        on="CNPJ",
        suffixes=(
            "_RECLAMACAO",
            "_CLIENTE"
        )
    )

    # Tratamentos finais
    df_final["DATA_PROCESSAMENTO"] = (
        pd.Timestamp.now()
    )

    df_final = df_final.drop_duplicates()

    arquivo = (
        DELIVERY_DIR /
        "reclamacoes_final.parquet"
    )

    df_final.to_parquet(
        arquivo,
        engine="pyarrow",
        index=False
    )

    print(
        f"Delivery criada: {arquivo}"
    )

    print(
        f"Total final: {len(df_final)}"
    )

    return df_final


if __name__ == "__main__":

    criar_delivery()
```

Executar:

```bash
python src/03_delivery.py
```

Resultado:

```text
data/delivery/reclamacoes_final.parquet
```

---

# 22. Carga da Delivery no PostgreSQL

O requisito determina que a camada Delivery também exista como uma tabela final dentro do banco relacional.

Tabela:

```text
delivery.reclamacoes_final
```

---

# 23. Script de carga

Criar `src/04_carga_delivery.py`:

```python
from pathlib import Path
import pandas as pd

from config import engine


DELIVERY_FILE = (
    Path("data/delivery")
    / "reclamacoes_final.parquet"
)


def carregar_delivery():

    print(
        "Lendo Delivery Parquet..."
    )

    df = pd.read_parquet(
        DELIVERY_FILE
    )

    print(
        f"Registros: {len(df)}"
    )

    print(
        "Carregando PostgreSQL..."
    )

    df.to_sql(
        name="reclamacoes_final",
        con=engine,
        schema="delivery",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method="multi"
    )

    print(
        "Tabela delivery.reclamacoes_final "
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

# 24. Resultado final

## PostgreSQL

```text
PostgreSQL
|
+-- raw
|   +-- base_clientes
|   +-- base_reclamacoes
|
+-- delivery
    +-- reclamacoes_final
```

## Arquivos

```text
data/
|
+-- input/
|   +-- base_clientes.csv
|   +-- base_reclamacoes.csv
|
+-- trusted/
|   +-- base_clientes.parquet
|   +-- base_reclamacoes.parquet
|
+-- delivery/
    +-- reclamacoes_final.parquet
```

---

# 25. Validação

Criar `src/05_validacao.py`:

```python
import pandas as pd

from config import engine


def validar():

    print("\nVALIDAÇÃO DO PIPELINE")
    print("-" * 50)

    raw_clientes = pd.read_sql(
        """
        SELECT *
        FROM raw.base_clientes
        """,
        engine
    )

    raw_reclamacoes = pd.read_sql(
        """
        SELECT *
        FROM raw.base_reclamacoes
        """,
        engine
    )

    delivery = pd.read_sql(
        """
        SELECT *
        FROM delivery.reclamacoes_final
        """,
        engine
    )

    print(
        f"RAW clientes       : {len(raw_clientes)}"
    )

    print(
        f"RAW reclamações    : {len(raw_reclamacoes)}"
    )

    print(
        f"DELIVERY final     : {len(delivery)}"
    )

    print(
        f"Colunas DELIVERY   : {len(delivery.columns)}"
    )


if __name__ == "__main__":

    validar()
```

Executar:

```bash
python src/05_validacao.py
```

---

# 26. Pipeline completo

Criar `run_pipeline.py`:

```python
import subprocess
import sys


scripts = [
    "src/01_ingestao_raw.py",
    "src/02_trusted.py",
    "src/03_delivery.py",
    "src/04_carga_delivery.py"
]


def executar():

    for script in scripts:

        print()
        print("=" * 60)
        print(f"Executando: {script}")
        print("=" * 60)

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
    print("=" * 60)
    print("PIPELINE EXECUTADO COM SUCESSO")
    print("=" * 60)


if __name__ == "__main__":

    executar()
```

Executar todo o processo:

```bash
python run_pipeline.py
```

---

# 27. Ordem de execução

A ordem recomendada é:

```text
1. Docker PostgreSQL
        |
        v
2. Criar schemas
        |
        v
3. Colocar arquivos em data/input
        |
        v
4. 01_ingestao_raw.py
        |
        v
5. 02_trusted.py
        |
        v
6. 03_delivery.py
        |
        v
7. 04_carga_delivery.py
        |
        v
8. 05_validacao.py
```

Ou simplesmente:

```bash
python run_pipeline.py
python src/05_validacao.py
```

---

# 28. Regras de tratamento

Todos os tratamentos devem ser realizados utilizando Python/Pandas.

Exemplos:

## Normalização de CNPJ

```python
df["CNPJ"] = df["CNPJ"].apply(
    normalizar_cnpj
)
```

## Normalização de nomes

```python
df["NOME"] = df["NOME"].apply(
    normalizar_nome
)
```

## Datas

```python
df["DATA_RECLAMACAO"] = pd.to_datetime(
    df["DATA_RECLAMACAO"],
    errors="coerce",
    dayfirst=True
)
```

## Duplicidades

```python
df = df.drop_duplicates()
```

## JOIN

```python
df_final = reclamacoes.merge(
    clientes,
    how="left",
    on="CNPJ"
)
```

---

# 29. Por que utilizar Parquet?

O Parquet foi escolhido para Trusted e Delivery porque:

- É um formato colunar;
- Possui boa compressão;
- É eficiente para leitura analítica;
- Preserva tipos de dados;
- É amplamente utilizado em arquiteturas de dados;
- É suportado pelo Pandas através do PyArrow.

Assim:

```text
RAW
Formato original / PostgreSQL

Trusted
Parquet

Delivery
Parquet + tabela PostgreSQL
```

---

# 30. Atendimento aos requisitos do enunciado

| Requisito | Implementação |
|---|---|
| Linguagem Python | OK |
| Ingestão com Python | Pandas |
| Tratamento com Python | Pandas |
| Pacotes adicionais | Pandas, PyArrow, SQLAlchemy etc. |
| Spark | Não utilizado |
| DuckDB | Não utilizado |
| Banco relacional open source | PostgreSQL |
| Todas as bases ingeridas | Sim |
| Tabela final | `delivery.reclamacoes_final` |
| JOIN em Python | `pandas.merge()` |
| RAW | PostgreSQL |
| Trusted | Parquet |
| Delivery | Parquet |
| Delivery dentro do banco | PostgreSQL |
| Tratamento via SQL | Não realizado |
| Rastreabilidade | RAW preservada |

---

# 31. Texto para documentação acadêmica

## Ingestão e ETL utilizando Python

Foi desenvolvida uma solução de ingestão e processamento de dados utilizando a linguagem Python, com as bibliotecas Pandas e PyArrow, sem utilização das tecnologias Apache Spark ou DuckDB.

Os dados foram inicialmente ingeridos em seu formato original para uma camada RAW armazenada em banco de dados PostgreSQL, garantindo a preservação dos dados de origem.

Posteriormente, os dados foram extraídos da camada RAW e submetidos a processos de tratamento utilizando exclusivamente Python, incluindo padronização de nomes de colunas, tratamento de valores nulos, conversão de tipos, normalização de CNPJ e nomes, tratamento de datas e remoção de registros duplicados.

Os dados tratados foram armazenados na camada TRUSTED no formato Parquet, utilizando a biblioteca PyArrow.

Na etapa seguinte, os arquivos Parquet da camada TRUSTED foram carregados utilizando Pandas e submetidos ao processo de integração das bases. O JOIN entre as bases foi realizado exclusivamente em Python, utilizando a função `merge()` do Pandas, não sendo utilizado SQL para as regras de transformação ou integração dos dados.

Após a integração, foi gerada a camada DELIVERY, também no formato Parquet, contendo o conjunto final de dados tratados e unidos.

Por fim, o conjunto de dados da camada DELIVERY foi carregado novamente no PostgreSQL, originando a tabela final `delivery.reclamacoes_final`, atendendo ao requisito de disponibilização da camada Delivery também como uma tabela em banco de dados relacional open source.

Dessa forma, a arquitetura implementada contempla as camadas RAW, TRUSTED e DELIVERY, com os processos de ingestão, tratamento, integração e transformação realizados através da linguagem Python.

---

# 32. Checklist final

Antes da entrega:

- [ ] PostgreSQL funcionando
- [ ] Docker funcionando
- [ ] Python 3.11 instalado
- [ ] Ambiente virtual criado
- [ ] `requirements.txt` instalado
- [ ] `.env` configurado
- [ ] Bases colocadas em `data/input`
- [ ] Schema RAW criado
- [ ] Schema Delivery criado
- [ ] RAW carregada
- [ ] Trusted em Parquet
- [ ] JOIN realizado em Python
- [ ] Delivery em Parquet
- [ ] Delivery carregada no PostgreSQL
- [ ] Tabela `delivery.reclamacoes_final` criada
- [ ] Validação executada
- [ ] README atualizado
- [ ] `.env` removido do versionamento

---

# 33. Comando principal

Depois de configurar o ambiente, o pipeline completo pode ser executado com:

```bash
python run_pipeline.py
```

E a validação:

```bash
python src/05_validacao.py
```

---

## Conclusão

A solução atende aos requisitos do exercício ao utilizar Python como linguagem central do processo de ingestão, tratamento e integração dos dados.

A arquitetura implementa:

```text
RAW      -> PostgreSQL
TRUSTED  -> Parquet
DELIVERY -> Parquet
DELIVERY -> PostgreSQL / tabela final
```

O tratamento e o JOIN são realizados com Python/Pandas, sem utilização de Spark, DuckDB ou SQL para transformação dos dados.
