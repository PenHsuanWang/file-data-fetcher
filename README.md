# CSV & Excel File Monitor Tool

## Overview
This tool monitors a designated folder (which can be a mount point to a NAS), detects the creation of new CSV and Excel files, and automatically processes the files by saving their content into a specified destination database (MongoDB, PostgreSQL, or Snowflake). It supports periodic polling for optimal performance and offers separate handlers for different file formats and databases.

### Features
- Supports both CSV and Excel (`.xls`/`.xlsx`) file formats.
- Modular design with handlers for MongoDB, PostgreSQL, and Snowflake.
- Polling-based folder monitoring for real-time file detection.
- Extensible and flexible architecture following the **Strategy Pattern**.
- Configurable via a `settings.py` file for easy customization.
  
## Folder Structure
```text
.
├── config/
│   └── settings.py           # Configuration and constants (MongoDB, PostgreSQL, Snowflake, folder path, etc.)
│
├── logs/
│   └── file_monitor.log      # Log files (auto-generated)
│
├── src/
│   ├── handlers/
│   │   ├── file_handlers/
│   │   │   ├── csv_handler.py    # CSV file processing logic
│   │   │   ├── excel_handler.py  # Excel file processing logic
│   │   │   └── base_file_handler.py # Abstract class/interface for file handlers
│   │   ├── db_handlers/
│   │   │   ├── mongo_handler.py  # MongoDB interaction logic
│   │   │   ├── postgres_handler.py # PostgreSQL interaction logic
│   │   │   ├── snowflake_handler.py # Snowflake interaction logic
│   │   │   └── base_db_handler.py  # Abstract class/interface for database handlers
│   │   └── folder_monitor.py     # Folder monitoring logic (polling + file detection)
│   │
│   ├── utils/
│   │   └── logger.py         # Centralized logging utility
│   │
│   └── main.py               # Main entry point for running the tool
│
├── tests/
│   ├── test_csv_handler.py    # Unit tests for CSV handler
│   ├── test_excel_handler.py  # Unit tests for Excel handler
│   ├── test_folder_monitor.py # Unit tests for folder monitor
│   ├── test_mongo_handler.py  # Unit tests for MongoDB handler
│   ├── test_postgres_handler.py # Unit tests for PostgreSQL handler
│   └── test_snowflake_handler.py # Unit tests for Snowflake handler
│
├── requirements.txt          # External dependencies (e.g., Pandas, openpyxl, psycopg2, snowflake-connector)
├── README.md                 # Project documentation
└── setup.py                  # Setup script for installing the tool (optional)
```

## Getting Started

### Prerequisites
- **Python 3.9+** 
- Install the following libraries via `pip`:
  - `pandas`: for file processing (CSV/Excel)
  - `watchdog`: for monitoring file system events
  - `openpyxl`: for handling `.xlsx` Excel files
  - `psycopg2`: for PostgreSQL interaction
  - `snowflake-connector-python`: for Snowflake interaction
  - `pymongo`: for MongoDB interaction

### Setup to Use

1. Clone the repository:

    ```bash
    git clone https://github.com/PenHsuanWang/file-data-fetcher.git
    ```

2. Navigate to the project directory:

    ```bash
    cd file-data-fetcher
    ```

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

4. Create a .env file to store your configuration:

    Run the following command to generate a .env file:
    
    ```bash
    touch .env
    ``` 
   Update the .env file with your database credentials and folder settings:
    ```env
    # Folder Monitoring
    FOLDER_TO_MONITOR=/path/to/folder
    POLL_INTERVAL=5  # in seconds
    
    # MongoDB Configuration
    MONGODB_URI=mongodb://localhost:27017/
    MONGODB_DATABASE=mydatabase
    MONGODB_COLLECTION=mycollection
    
    # PostgreSQL Configuration
    POSTGRESQL_DBNAME=your_db
    POSTGRESQL_USER=your_user
    POSTGRESQL_PASSWORD=your_password
    POSTGRESQL_HOST=localhost
    POSTGRESQL_PORT=5432
    
    # Snowflake Configuration
    SNOWFLAKE_USER=your_user
    SNOWFLAKE_PASSWORD=your_password
    SNOWFLAKE_ACCOUNT=your_account
    SNOWFLAKE_WAREHOUSE=your_warehouse
    SNOWFLAKE_DATABASE=your_database
    SNOWFLAKE_SCHEMA=your_schema
    SNOWFLAKE_ROLE=your_role
    ```
    Ensure that .env is Git-ignored by confirming the presence of .env in the .gitignore file.

5.	Configure the settings.py to load variables from .env:
    In config/settings.py, update the file to use python-dotenv to load environment variables:
    
    ```python
    from dotenv import load_dotenv
    import os
    
    # Load environment variables from .env
    load_dotenv()
    
    FOLDER_TO_MONITOR = os.getenv("FOLDER_TO_MONITOR")
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL"))
    
    # MongoDB configuration
    MONGODB_URI = os.getenv("MONGODB_URI")
    MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")
    
    # PostgreSQL configuration
    POSTGRESQL = {
        'dbname': os.getenv("POSTGRESQL_DBNAME"),
        'user': os.getenv("POSTGRESQL_USER"),
        'password': os.getenv("POSTGRESQL_PASSWORD"),
        'host': os.getenv("POSTGRESQL_HOST"),
        'port': os.getenv("POSTGRESQL_PORT"),
    }
    
    # Snowflake configuration
    SNOWFLAKE = {
        'user': os.getenv("SNOWFLAKE_USER"),
        'password': os.getenv("SNOWFLAKE_PASSWORD"),
        'account': os.getenv("SNOWFLAKE_ACCOUNT"),
        'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
        'database': os.getenv("SNOWFLAKE_DATABASE"),
        'schema': os.getenv("SNOWFLAKE_SCHEMA"),
        'role': os.getenv("SNOWFLAKE_ROLE"),
    }
    ```

### Usage

To run the tool, execute the following command:

```bash
python src/main.py
```

This will start monitoring the folder specified in `config/settings.py`. When a new CSV or Excel file is created, the tool will process it and save the records into the specified database (MongoDB, PostgreSQL, or Snowflake).

### Customization

#### File Handlers
You can extend the functionality by adding new file handlers. Implement a new handler by inheriting from the `BaseFileHandler` interface located in `src/handlers/file_handlers/base_file_handler.py`.

#### Database Handlers
If you want to support additional databases (e.g., MySQL), you can create new handlers by inheriting from `BaseDBHandler` located in `src/handlers/db_handlers/base_db_handler.py`.

### Testing

Run the unit tests to ensure everything works as expected:

```bash
pytest --cov tests
```

## Dependencies
- **pandas**: Data manipulation for CSV/Excel files.
- **watchdog**: Real-time folder monitoring.
- **pymongo**: MongoDB integration.
- **psycopg2**: PostgreSQL database interaction.
- **snowflake-connector-python**: Snowflake database interaction.
- **openpyxl**: Excel file processing.

For a full list of dependencies, refer to the `requirements.txt` file.


