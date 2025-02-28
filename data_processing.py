import json
import os
import logging
from main import DataMigration, DatabaseConnector, DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    db = DatabaseConnector(DB_CONFIG)
    db.connect()

    migration = DataMigration(db)

    # Bu kisimda bir klasor icerisindeki tum json dosyalarinin okunmasi saglaniyor.
    directory =  "C:/Users/ilker/Desktop/DataEngineeringWEG"  # JSON dosyalarının olduğu klasör

    json_files = [f for f in os.listdir(directory) if f.endswith(".json")]

    if not json_files:
        logging.error("Error: No JSON files found in the directory!")
    else:
        for json_file in json_files:
            file_path = os.path.join(directory, json_file)
            try:
                with open(file_path, "r") as file:
                    json_data = json.load(file)
                migration.process_json(json.dumps(json_data))
                logging.info(f"Successfully processed {json_file}.")
            except Exception as e:
                logging.error(f"Error processing {json_file}: {e}")
    
    # Eger kullanicidan manuel dosya yolu istersek bu bolumu kullanabiliriz.
    # file_path = input("Please enter the JSON file path: ") 
    # if os.path.exists(file_path):
    #     with open(file_path, "r") as json_file:
    #         json_data = json.load(json_file)
    #     migration.process_json(json.dumps(json_data))
    #     logging.info(f"Successfully processed {file_path}.")
    # else:
    #     logging.error("Error: File not found!")

    db.close()
