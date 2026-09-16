import json
import time
import pika
from datetime import datetime


RABBITMQ_HOST = "rabbitmq"

QUEUE_NAME = "eventos_reclamacoes"


connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST
    )
)

channel = connection.channel()

channel.queue_declare(
    queue=QUEUE_NAME,
    durable=True
)


evento = {
    "event_id": "evt-000001",
    "timestamp": datetime.utcnow().isoformat(),
    "tipo": "reclamacao",
    "cnpj": "00000000000100",
    "instituicao": "BANCO TESTE",
    "quantidade": 10
}


channel.basic_publish(
    exchange="",
    routing_key=QUEUE_NAME,
    body=json.dumps(evento),
    properties=pika.BasicProperties(
        delivery_mode=2
    )
)


print("Evento enviado:")
print(evento)


connection.close()