import json
import logging
import psycopg2

# Loglama yapilandirmasi (calistirma zamani,hata mesaji)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Veritabanı bilgileri icin
dbname = input("Enter database name: ")
user = input("Enter username: ")
password = input("Enter password: ")  
host = input("Enter host (default: localhost): ") or "localhost"
port = input("Enter port (default: 5432): ") or "5432"

# Database ayarlari
DB_CONFIG = {
    "dbname": dbname,
    "user": user,
    "password": password,
    "host": host,
    "port": port
}


class DatabaseConnector:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.config)
            logging.info("Database connection established.")
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise

    def execute_query(self, query, data=None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, data)
                self.conn.commit()
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            self.conn.rollback()

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

class Category:
    def __init__(self, category_id, name):
        self.category_id = category_id
        self.name = name

class Chain:
    def __init__(self, chain_id, name):
        self.chain_id = chain_id
        self.name = name

class Hotel:
    def __init__(self, hotel_id, name, category_id, chain_id, location):
        self.hotel_id = hotel_id
        self.name = name
        self.category_id = category_id
        self.chain_id = chain_id
        self.location = location

class DataMigration:
    def __init__(self, db_connector):
        self.db_connector = db_connector
    
    def create_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS Category (
                category_id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            )""",
            """
            CREATE TABLE IF NOT EXISTS Chain (
                chain_id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            )""",
            """
            CREATE TABLE IF NOT EXISTS Hotel (
                hotel_id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category_id INTEGER REFERENCES Category(category_id),
                chain_id INTEGER REFERENCES Chain(chain_id),
                location VARCHAR(255)
            )"""
        ]
        for query in queries:
            self.db_connector.execute_query(query)
        logging.info("Database tables created successfully.")
    
    def insert_category(self, category):
        query = "INSERT INTO Category (category_id, name) VALUES (%s, %s) ON CONFLICT (category_id) DO UPDATE SET name = EXCLUDED.name;;"
        self.db_connector.execute_query(query, (category.category_id, category.name))
    
    def insert_chain(self, chain):
        query = "INSERT INTO Chain (chain_id, name) VALUES (%s, %s) ON CONFLICT (chain_id) DO UPDATE SET name = EXCLUDED.name;"
        self.db_connector.execute_query(query, (chain.chain_id, chain.name))
    
    # ayni otel id'si insert oldugu zaman eger isim ve konum degistiyse, otelin ismi ve konumu guncellenecek.
    def insert_hotel(self, hotel):
        query = "INSERT INTO Hotel (hotel_id, name, category_id, chain_id, location) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hotel_id) DO UPDATE SET name = EXCLUDED.name, location = EXCLUDED.location;"
        self.db_connector.execute_query(query, (hotel.hotel_id, hotel.name, hotel.category_id, hotel.chain_id, hotel.location))

    def process_json(self, json_data):
        try:
            data = json.loads(json_data)
            categories = {c["id"]: Category(c["id"], c["name"]) for c in data["categories"]}
            chains = {c["id"]: Chain(c["id"], c["name"]) for c in data["chains"]}
            hotels = [Hotel(h["id"], h["name"], h["category_id"], h["chain_id"], h["location"]) for h in data["hotels"]]
            
            for category in categories.values():
                self.insert_category(category)
            for chain in chains.values():
                self.insert_chain(chain)
            for hotel in hotels:
                self.insert_hotel(hotel)
            
            logging.info("Data migration completed successfully.")
        except Exception as e:
            logging.error(f"Error processing JSON: {e}")

if __name__ == "__main__":
    db = DatabaseConnector(DB_CONFIG)
    db.connect()

    migration = DataMigration(db)
    migration.create_tables()

    db.close()
