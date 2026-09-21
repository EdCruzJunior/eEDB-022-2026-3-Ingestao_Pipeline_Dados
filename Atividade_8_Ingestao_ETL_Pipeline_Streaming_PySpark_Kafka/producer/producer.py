import csv
import json
import os
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "reclamacoes")
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DELAY = float(os.getenv("PRODUCER_DELAY", "0.15"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    retries=10,
)

csv_files = sorted(DATA_DIR.glob("*.csv"))
if not csv_files:
    raise RuntimeError(f"Nenhum CSV encontrado em {DATA_DIR}")

print(f"[PRODUTOR] Kafka={KAFKA_BOOTSTRAP} tópico={TOPIC}")
print(f"[PRODUTOR] Arquivos: {[p.name for p in csv_files]}")

total = 0

for path in csv_files:
    # Arquivos da atividade estão em CSV separado por ; e codificação Latin-1.
    with path.open("r", encoding="latin1", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row_number, row in enumerate(reader, start=2):
            # Remove coluna vazia gerada por alguns arquivos.
            row = {k: v for k, v in row.items() if k and not k.startswith("Unnamed")}
            event = {
                "event_id": str(uuid.uuid4()),
                "source_file": path.name,
                "source_row": row_number,
                "ingestion_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "data": row,
            }
            producer.send(TOPIC, value=event)
            total += 1
            if total % 100 == 0:
                producer.flush()
                print(f"[PRODUTOR] enviados={total}")
            time.sleep(DELAY)

producer.flush()
producer.close()
print(f"[PRODUTOR] Finalizado. Total de mensagens enviadas: {total}")
