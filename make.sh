make clean
make pipeline
docker compose up -d
make postgres
make indexes
