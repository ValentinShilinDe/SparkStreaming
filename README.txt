docker start
 docker-compose -f .\docker-compose.yml up -d
docker down
docker-compose -f .\docker-compose.yml down

Arguments:
- Consumer
    localhost:9092 books src/main/resources/parquet/
- Producer
    src/main/resources/ localhost:9092 books