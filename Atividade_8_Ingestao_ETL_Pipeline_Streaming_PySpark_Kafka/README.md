# Pipeline Streaming com PySpark + Kafka + PostgreSQL

Projeto local em Docker para a atividade de Streaming.

## Arquitetura

Arquivos CSV locais -> Python Producer -> Apache Kafka -> PySpark Structured Streaming -> PostgreSQL para enriquecimento -> Parquet local.

### Componentes
- Apache Kafka 3.7 em Docker/KRaft
- PostgreSQL 16 em Docker
- Python Producer
- PySpark 3.5.1 Structured Streaming
- Dados de enquadramento (`EnquadramentoInicia_v2`) usados como dimensão SQL
- Dados Glassdoor usados como enriquecimento complementar
- Saída em Parquet no diretório `output/`

## 1. Pré-requisitos

- Docker Desktop instalado e em execução
- Docker Compose v2
- Pelo menos 4 GB de RAM disponíveis para Docker

Teste:

```bash
docker --version
docker compose version
```

## 2. Estrutura

```text
pipeline_streaming_pyspark_kafka/
├── data/
│   ├── 2021_tri_01(7).csv
│   ├── ...
│   ├── 2022_tri_04(7).csv
│   ├── EnquadramentoInicia_v2(6).tsv
│   └── glassdoor_consolidado_join_match_v2(6).csv
├── producer/
│   ├── producer.py
│   └── requirements.txt
├── consumer/
│   ├── consumer.py
│   └── requirements.txt
├── db/
│   └── init_db.py
├── output/
├── checkpoint/
├── docker-compose.yml
└── README.md
```

## 3. Subir Kafka e PostgreSQL

No PowerShell, CMD ou terminal Linux:

```bash
docker compose up -d kafka postgres
docker compose ps
```

Aguarde os healthchecks.

Criar o tópico:

```bash
docker compose run --rm kafka-init
```

Criar e carregar as tabelas SQL:

```bash
docker compose run --rm db-init
```

## 4. Conferir o PostgreSQL

Entrar no banco:

```bash
docker exec -it streaming-postgres psql -U streaming -d streaming
```

Executar:

```sql
\dt

SELECT COUNT(*) FROM institution_enrichment;
SELECT COUNT(*) FROM glassdoor_enrichment;

SELECT * FROM institution_enrichment LIMIT 10;
SELECT * FROM glassdoor_enrichment LIMIT 10;
```

Sair:

```sql
\q
```

O PostgreSQL fica disponível no host em `localhost:5434`.

## 5. Iniciar o consumidor PySpark

Em um terminal:

```bash
docker compose run --rm spark-consumer
```

O job:
1. lê o tópico Kafka;
2. desserializa o JSON;
3. processa cada micro-batch;
4. extrai CNPJ e nome;
5. consulta PostgreSQL;
6. faz o join do resultado SQL com a janela Spark;
7. calcula `taxa_reclamacoes_por_100k_clientes`;
8. grava Parquet em `output/`.

Deixe esse terminal executando.

## 6. Iniciar o produtor

Em outro terminal:

```bash
docker compose run --rm producer
```

O produtor lê todos os CSVs da pasta `data/`, preserva os campos originais e adiciona:
- `event_id`
- `source_file`
- `source_row`
- `ingestion_ts`

Cada registro vira uma mensagem JSON no tópico `reclamacoes`.

## 7. Observar Kafka

Ver mensagens:

```bash
docker exec -it streaming-kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic reclamacoes   --from-beginning
```

Listar o tópico:

```bash
docker exec -it streaming-kafka /opt/bitnami/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092 --describe --topic reclamacoes
```

## 8. Conferir os arquivos enriquecidos

Depois do produtor terminar e os micro-batches serem processados:

```bash
ls output
```

Windows PowerShell:

```powershell
Get-ChildItem .\output
```

Os dados estarão em Parquet.

Exemplo de consulta com Python/Pandas:

```python
import pandas as pd
df = pd.read_parquet("output")
print(df.head())
print(df.columns.tolist())
```

## 9. Conferir o enriquecimento

O resultado contém, entre outros:

- dados da reclamação;
- CNPJ normalizado;
- `segmento`;
- `nome_enquadramento`;
- notas Glassdoor;
- percentuais Glassdoor;
- `taxa_reclamacoes_por_100k_clientes`;
- `batch_id`;
- `processed_ts`.

Uma linha conceitual fica assim:

```text
CSV -> Kafka JSON -> Spark micro-batch
                     |
                     +--> PostgreSQL
                           |
                           +--> segmento / enquadramento
                           +--> Glassdoor
                     |
                     +--> Join
                     |
                     +--> Parquet
```

## 10. Como demonstrar a janela do Structured Streaming

O consumidor usa:

```python
.trigger(processingTime="10 seconds")
```

Isso cria micro-batches aproximadamente a cada 10 segundos enquanto houver dados.

A lógica de enriquecimento está dentro de:

```python
def process_batch(batch_df, batch_id):
```

Isso atende ao requisito de receber uma janela de informações e fazer a consulta SQL para enriquecê-la.

## 11. Reset completo

Para repetir a atividade do zero:

```bash
docker compose down -v
```

Depois remova:

```bash
rm -rf output checkpoint
mkdir output checkpoint
```

No PowerShell:

```powershell
docker compose down -v
Remove-Item -Recurse -Force output, checkpoint
New-Item -ItemType Directory output
New-Item -ItemType Directory checkpoint
```

E execute novamente:

```bash
docker compose up -d kafka postgres
docker compose run --rm kafka-init
docker compose run --rm db-init
```

Depois inicie consumidor e produtor.

## 12. Observações sobre os arquivos

Os CSVs da atividade são tratados como `;` e `latin1`, enquanto o TSV de enquadramento é `UTF-8`.

Foi usado o CNPJ como chave primária de enriquecimento quando disponível. Para os dados Glassdoor, o enriquecimento complementar usa o nome normalizado da instituição.

O arquivo `2022_tri_02` não está presente entre os anexos recebidos; o pipeline processa todos os arquivos disponíveis na pasta `data/`.

## 13. Evidências para entrega acadêmica

Recomenda-se capturar:
1. `docker compose ps`;
2. tópico Kafka criado;
3. mensagens no Kafka;
4. logs do produtor;
5. logs do Spark;
6. consulta das tabelas PostgreSQL;
7. arquivos Parquet em `output/`;
8. amostra dos dados finais enriquecidos.

## 14. Comandos principais resumidos

Terminal 1:

```bash
docker compose up -d kafka postgres
docker compose run --rm kafka-init
docker compose run --rm db-init
```

Terminal 2:

```bash
docker compose run --rm spark-consumer
```

Terminal 3:

```bash
docker compose run --rm producer
```

Terminal 4, opcional:

```bash
docker exec -it streaming-kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic reclamacoes
```
