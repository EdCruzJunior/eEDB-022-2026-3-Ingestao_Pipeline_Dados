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


## 7A. Kafka UI – Monitoramento visual do Kafka

Além dos comandos de terminal, o projeto utiliza a **Kafka UI v0.7.2** para acompanhar visualmente o cluster, os tópicos e as mensagens.

### 7A.1 Iniciar a Kafka UI

```powershell
docker compose up -d kafka-ui
docker compose ps
```

A porta publicada deve aparecer como:

```text
0.0.0.0:8080->8080/tcp
```

### 7A.2 Acessar a interface

Abrir no navegador:

```text
http://localhost:8080
```

O cluster utilizado é:

```text
local-kafka
```

### 7A.3 Consultar o tópico `reclamacoes`

Na Kafka UI:

1. Acessar **Topics**.
2. Selecionar **reclamacoes**.
3. Abrir **Messages**.
4. Verificar as mensagens produzidas pelo `producer.py`.
5. Observar **Partition**, **Offset** e conteúdo da mensagem.

Conceitos demonstrados:

- **Topic:** canal lógico de eventos.
- **Partition:** divisão do tópico para paralelismo.
- **Offset:** posição da mensagem dentro da partição.
- **Message:** evento publicado pelo Producer.

A Kafka UI constitui uma evidência visual da etapa:

```text
Python Producer
      |
      v
Kafka Topic: reclamacoes
      |
      +--> Partitions
      +--> Offsets
      +--> Messages
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


## 10A. Spark UI – Monitoramento do PySpark Structured Streaming

O projeto utiliza a **Spark UI** para demonstrar visualmente a execução do consumidor PySpark e dos micro-batches.

### 10A.1 Configuração da porta 4040

No serviço `spark-consumer`, publicar:

```yaml
ports:
  - "4040:4040"
```

E utilizar no `spark-submit`:

```text
--master local[*]
--conf spark.ui.port=4040
--conf spark.ui.bindAddress=0.0.0.0
```

### 10A.2 Iniciar o consumidor

Para manter a Spark UI disponível:

```powershell
docker compose up spark-consumer
```

Deixe o terminal executando.

Verificar:

```powershell
docker port streaming-spark-consumer
```

Resultado esperado:

```text
4040/tcp -> 0.0.0.0:4040
```

### 10A.3 Acessar a Spark UI

Abrir:

```text
http://localhost:4040
```

A aplicação apresentada é:

```text
PipelineStreamingPySparkKafka
```

Abas relevantes:

- **Jobs**
- **Stages**
- **Storage**
- **Environment**
- **Executors**
- **SQL / DataFrame**
- **Structured Streaming**

### 10A.4 Acompanhar o processamento

Na aba **Jobs**, é possível acompanhar Jobs ativos e concluídos, duração, Stages e Tasks.

Na execução utilizada como evidência da atividade, a Spark UI apresentou:

```text
Spark 3.5.1
Active Jobs: 1
Completed Jobs: 4
batch = 42
Stages: 4/5
Tasks: 31/32
```

Também foram observadas tarefas concluídas como:

```text
7/7
7/7
1/1
```

Essa evidência demonstra que o consumidor PySpark Structured Streaming está efetivamente processando as mensagens recebidas do Kafka.

### 10A.5 Kafka UI x Spark UI

As duas interfaces são complementares:

```text
             KAFKA UI
          localhost:8080
                |
                v
     Topic: reclamacoes
                |
                v
      PySpark Structured
          Streaming
                |
                v
             SPARK UI
          localhost:4040
```

A **Kafka UI** demonstra a camada de mensageria; a **Spark UI** demonstra o processamento dos eventos em Jobs, Stages, Tasks e micro-batches.

### 10A.6 Consumer Group

Na aba **Consumers** da Kafka UI pode não aparecer um Consumer Group tradicional correspondente ao Spark Structured Streaming. Isso não significa que o Spark não esteja consumindo as mensagens.

Para a atividade, a evidência do consumidor deve ser apresentada em conjunto:

```text
Kafka UI
+
Spark UI
+
Logs do Spark
+
Arquivos Parquet
```

Não é necessário criar artificialmente um `group.id` apenas para fazer o Job Structured Streaming aparecer como Consumer Group tradicional.

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
3. **Kafka UI (`http://localhost:8080`) com o tópico `reclamacoes` e mensagens**;
4. mensagens no Kafka;
5. logs do produtor;
6. logs do Spark;
7. **Spark UI (`http://localhost:4040`) com Jobs, Stages, Tasks e micro-batch em processamento**;
8. consulta das tabelas PostgreSQL;
9. arquivos Parquet em `output/`;
10. amostra dos dados finais enriquecidos.

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


## 15. Roteiro integrado para a demonstração

### Terminal 1 – Infraestrutura

```powershell
docker compose up -d kafka postgres
docker compose run --rm kafka-init
docker compose run --rm db-init
docker compose up -d kafka-ui
```

### Terminal 2 – Spark Consumer

```powershell
docker compose up spark-consumer
```

Abrir:

```text
http://localhost:4040
```

### Terminal 3 – Producer

```powershell
docker compose run --rm producer
```

### Kafka UI

Abrir:

```text
http://localhost:8080
```

Navegar até:

```text
Topics
  -> reclamacoes
     -> Messages
```

### Spark UI

Abrir:

```text
http://localhost:4040
```

Navegar até:

```text
Jobs
```

e consultar também:

```text
Structured Streaming
```

### Fluxo demonstrado

```text
CSV/TSV
   |
   v
Python Producer
   |
   v
Kafka / reclamacoes
   |
   +--------------------> Kafka UI :8080
   |
   v
PySpark Structured Streaming
   |
   +--------------------> Spark UI :4040
   |
   v
PostgreSQL
   |
   v
Parquet
```

A demonstração evidencia separadamente a camada de mensageria e o processamento streaming, mantendo o PostgreSQL como fonte de enriquecimento e o Parquet como saída final.
