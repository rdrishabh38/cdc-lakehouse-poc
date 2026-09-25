import time
import random
from sqlalchemy import create_engine, text
from faker import Faker

# Connect to Postgres container network
engine = create_engine('postgresql://postgres:postgres@postgres:5432/postgres')
fake = Faker()

def generate_data():
    with engine.connect() as conn:
        print("Waiting a few seconds before seeding to ensure connector is active...")
        time.sleep(5)
        
        # Initial seed to trigger Debezium Schema Registration
        print("Inserting initial seed row to trigger schema registration...")
        conn.execute(text(
            "INSERT INTO inventory.customers (first_name, last_name, email, insert_timestamp, update_timestamp) VALUES (:first_name, :last_name, :email, NOW(), NULL)"
        ), {"first_name": "Initial", "last_name": "Seed", "email": "seed@example.com"})
        conn.commit()

        print("Waiting 15 seconds for Debezium to register schema and Spark to fetch it...")
        time.sleep(15)

        # Start continuous stream
        print("Starting continuous CDC stream...")
        while True:
            op = random.choices(['insert', 'update', 'delete'], weights=[0.6, 0.3, 0.1])[0]
            
            try:
                if op == 'insert':
                    first_name, last_name, email = fake.first_name(), fake.last_name(), fake.email()
                    conn.execute(text(
                        "INSERT INTO inventory.customers (first_name, last_name, email, insert_timestamp, update_timestamp) VALUES (:first_name, :last_name, :email, NOW(), NULL)"
                    ), {"first_name": first_name, "last_name": last_name, "email": email})
                    print(f"INSERT: {first_name} {last_name} ({email})")
                
                elif op == 'update':
                    result = conn.execute(text("SELECT id, first_name, last_name FROM inventory.customers ORDER BY RANDOM() LIMIT 1")).fetchone()
                    if result:
                        new_email = fake.email()
                        conn.execute(text(
                            "UPDATE inventory.customers SET email = :email, update_timestamp = NOW() WHERE id = :id"
                        ), {"email": new_email, "id": result[0]})
                        print(f"UPDATE [ID {result[0]}]: {result[1]} {result[2]} -> new_email={new_email}")
                
                elif op == 'delete':
                    result = conn.execute(text("SELECT id, first_name, last_name FROM inventory.customers ORDER BY RANDOM() LIMIT 1")).fetchone()
                    if result:
                        conn.execute(text(
                            "DELETE FROM inventory.customers WHERE id = :id"
                        ), {"id": result[0]})
                        print(f"DELETE [ID {result[0]}]: {result[1]} {result[2]}")
                        
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Error during {op}: {e}")
            
            # Simulate real-time traffic
            time.sleep(random.uniform(0.5, 2))

if __name__ == "__main__":
    generate_data()
