import urllib.request
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

import time

def get_schema_from_registry(topic_name, max_retries=60, retry_delay=5):
    """Fetches the latest Avro schema, with retry logic in case the topic is empty/unregistered yet."""
    url = f"http://schema-registry:8081/subjects/{topic_name}-value/versions/latest"
    req = urllib.request.Request(url)
    
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                return data['schema']
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"Schema for {topic_name} not found yet (attempt {attempt+1}/{max_retries}). Waiting for first message...")
                time.sleep(retry_delay)
            else:
                raise
        except urllib.error.URLError as e:
            print(f"Schema registry not reachable yet (attempt {attempt+1}/{max_retries}). Waiting...")
            time.sleep(retry_delay)
            
    raise Exception(f"Failed to fetch schema after {max_retries} attempts.")

def create_iceberg_table_if_not_exists(spark):
    """Creates the Iceberg table using a local Hadoop catalog"""
    spark.sql("""
    CREATE TABLE IF NOT EXISTS local.inventory.customers (
        id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        insert_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
    )
    """)

def process_microbatch(df, epoch_id):
    """Executes a MERGE INTO operation for each streaming micro-batch"""
    if df.isEmpty():
        return
        
    df.createOrReplaceTempView("cdc_microbatch")
    
    # We coalesce after.id and before.id to handle DELETES where 'after' is null
    merge_sql = """
    MERGE INTO local.inventory.customers t
    USING (
        SELECT 
            COALESCE(after.id, before.id) as id,
            after.first_name,
            after.last_name,
            after.email,
            after.insert_timestamp,
            after.update_timestamp,
            op
        FROM cdc_microbatch
    ) s
    ON t.id = s.id
    WHEN MATCHED AND s.op = 'd' THEN DELETE
    WHEN MATCHED AND s.op IN ('u', 'c') THEN UPDATE SET *
    WHEN NOT MATCHED AND s.op IN ('c', 'u') THEN INSERT *
    """
    df.sparkSession.sql(merge_sql)

if __name__ == "__main__":
    # Initialize SparkSession with Iceberg configurations
    spark = SparkSession.builder \
        .appName("CDC_Iceberg_Pipeline") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/lakehouse") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    print("Creating Iceberg table if not exists...")
    create_iceberg_table_if_not_exists(spark)
    
    topic = "dbserver1.inventory.customers"
    print(f"Fetching Avro schema for {topic}...")
    avro_schema = get_schema_from_registry(topic)
    
    print("Starting Kafka stream...")
    # Read stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
        
    # Strip the 5-byte Confluent wire format prefix and parse Avro
    # Format: 1 byte Magic Byte + 4 bytes Schema ID + Avro payload
    parsed_df = kafka_df.select(
        from_avro(expr("substring(value, 6)"), avro_schema).alias("data")
    ).select("data.*")
    
    # Filter valid CDC operations
    valid_ops_df = parsed_df.filter(col("op").isin("c", "u", "d"))
    
    # Write to Iceberg using foreachBatch
    query = valid_ops_df.writeStream \
        .foreachBatch(process_microbatch) \
        .outputMode("update") \
        .start()
        
    query.awaitTermination()
