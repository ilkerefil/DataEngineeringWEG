# Data Pipeline Documentation

## Overview
This data pipeline is designed to process and migrate JSON-based hotel data into a PostgreSQL database. It includes components for database connectivity, data transformation, and structured logging to ensure efficient data processing and debugging.

## Architecture
The pipeline consists of the following core components:

1. **DatabaseConnector** (in `main.py`): Handles the connection to the PostgreSQL database.
2. **DataMigration** (in `main.py`): Manages the creation of database tables and insertion of hotel, chain, and category data.
3. **Batch Processing** (in `data_processing.py`): Reads multiple JSON files from a specified directory and processes them.
4. **Unit Testing** (in `test.py`): Ensures data processing functions correctly through unit tests.

## Design Decisions
- **PostgreSQL as the Database**: Chosen for its reliability, scalability, and robust relational data management capabilities.
- **JSON Input Format**: Allows for structured data ingestion, making it easier to integrate with various data sources.
- **Logging**: Configured at the INFO level to track data migration steps and errors.
- **Conflict Resolution**: Uses PostgreSQL's `ON CONFLICT` mechanism to update existing records when necessary, ensuring data consistency.
- **Batch Processing**: Enables processing multiple JSON files in `data_processing.py` for efficiency.
- **Unit Testing with `unittest`**: Ensures the integrity of data migration operations before deployment.

## Implementation Details
### DatabaseConnector
- Connects to PostgreSQL using credentials provided by the user.
- Implements methods for executing queries and handling errors.

### DataMigration
- **Creates necessary tables** (`Category`, `Chain`, `Hotel`) if they do not exist.
- **Processes JSON input** by extracting `categories`, `chains`, and `hotels`, inserting them into the database.
- **Handles conflicts** using `ON CONFLICT` to update records without duplicating data.

### Testing (test.py)
- Sets up a temporary test database.
- Inserts sample JSON data and verifies correct insertion.
- Cleans up test data after execution.

### Batch Processing (data_processing.py)
- Reads all JSON files from a predefined directory.
- Processes each file through `DataMigration.process_json()`.
- Uses logging to track progress and errors.

## Usage
1. Ensure PostgreSQL is running and accessible.
2. Run `main.py` to establish the database connection and create tables.
3. Run `test.py` to verify data integrity.
4. Use `data_processing.py` to process JSON files.

