START DOCKER
docker run -d --name=kafka -p 9092:9092 apache/kafka

STOP AND DELETE DOCKER
docker compose down -v && docker compose delete
docker rm -f kafka
