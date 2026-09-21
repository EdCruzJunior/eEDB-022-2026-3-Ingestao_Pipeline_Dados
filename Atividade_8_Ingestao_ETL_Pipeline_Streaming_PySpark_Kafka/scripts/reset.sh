#!/bin/sh
docker compose down -v
rm -rf output checkpoint
mkdir -p output checkpoint
docker compose up -d kafka postgres
docker compose run --rm kafka-init
docker compose run --rm db-init
