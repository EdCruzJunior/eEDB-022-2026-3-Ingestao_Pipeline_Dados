# Comandos PowerShell - atividade
docker compose up -d kafka postgres
docker compose run --rm kafka-init
docker compose run --rm db-init

# Terminal separado:
docker compose run --rm spark-consumer

# Outro terminal:
docker compose run --rm producer

# Consultar PostgreSQL:
docker exec -it streaming-postgres psql -U streaming -d streaming

# Reset:
docker compose down -v
Remove-Item -Recurse -Force output, checkpoint
New-Item -ItemType Directory output
New-Item -ItemType Directory checkpoint
