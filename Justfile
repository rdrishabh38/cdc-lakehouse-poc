# Justfile for CDC Lakehouse POC

# Start the infrastructure
up:
    docker compose up -d

# Stop the infrastructure
down:
    docker compose down

# View logs for all or a specific service (e.g. `just logs kafka-connect`)
logs service="":
    docker compose logs -f {{service}}

# Deploy the Debezium Postgres source connector
deploy-connector:
    curl -X POST -H "Content-Type: application/json" --data @docker/kafka-connect/postgres-source-connector.json http://localhost:8083/connectors
    
# Check connector status
connector-status:
    curl -s http://localhost:8083/connectors/postgres-source/status | jq .

# Delete the connector
delete-connector:
    curl -X DELETE http://localhost:8083/connectors/postgres-source

# show containers
status:
    docker ps -a

# Query the Iceberg Lakehouse for the latest 10 updated records
query-lakehouse:
    docker exec -it spark-app /opt/spark/bin/spark-sql \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.local.type=hadoop \
      --conf spark.sql.catalog.local.warehouse=/tmp/lakehouse \
      -e "SELECT id, first_name, email, insert_timestamp, update_timestamp FROM local.inventory.customers ORDER BY COALESCE(update_timestamp, insert_timestamp) DESC LIMIT 10;"

# Query the Postgres Source database for the latest 10 updated records
query-postgres:
    docker exec -it postgres psql -U postgres -d postgres -c "SELECT id, first_name, email, insert_timestamp, update_timestamp FROM inventory.customers ORDER BY COALESCE(update_timestamp, insert_timestamp) DESC LIMIT 10;"
