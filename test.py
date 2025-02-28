import unittest
from main import DataMigration, DatabaseConnector, DB_CONFIG

class TestDataMigration(unittest.TestCase):
    def setUp(self):
        """Set up the database connection and create tables before each test."""
        self.db = DatabaseConnector(DB_CONFIG)
        self.db.connect()
        self.migration = DataMigration(self.db)
        self.migration.create_tables()
    
    def tearDown(self):
        """Remove test data and close the database connection after each test."""
        with self.db.conn.cursor() as cur:
            cur.execute("DELETE FROM Hotel;")
            cur.execute("DELETE FROM Chain;")
            cur.execute("DELETE FROM Category;")
            self.db.conn.commit()
        self.db.close()
    
    def test_process_json(self):
        """Test if JSON data is processed correctly and inserted into the database."""
        sample_json = '{"categories": [{"id": 1, "name": "Luxury"}], "chains": [{"id": 1, "name": "Hilton"}], "hotels": [{"id": 1, "name": "Hilton NYC", "category_id": 1, "chain_id": 1, "location": "New York"}]}'
        self.migration.process_json(sample_json)
        
        # Veriler database'e işlendi mi?
        with self.db.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM Category WHERE name = 'Luxury';")
            category_count = cur.fetchone()[0]
            self.assertEqual(category_count, 1)
            
            cur.execute("SELECT COUNT(*) FROM Chain WHERE name = 'Hilton';")
            chain_count = cur.fetchone()[0]
            self.assertEqual(chain_count, 1)
            
            cur.execute("SELECT COUNT(*) FROM Hotel WHERE name = 'Hilton NYC';")
            hotel_count = cur.fetchone()[0]
            self.assertEqual(hotel_count, 1)

if __name__ == "__main__":
    unittest.main()
