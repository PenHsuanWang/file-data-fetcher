# CSV & Excel File Monitor Tool

## Overview

This tool monitors a specific folder (which can also be a NAS mount point) for the creation or modification of new CSV and Excel files. Upon detecting a file, the tool processes its content and inserts the data into a target database—MongoDB, PostgreSQL, or Snowflake. It leverages a modular design, where each file format and database interaction is handled through specific classes, ensuring scalability and maintainability.

The project utilizes **asynchronous processing** via `asyncio` and `watchdog` for real-time folder monitoring. It adopts well-established software design patterns like the **Strategy Pattern** and **Template Method**. Highly configurable through a `.env` file, the tool can be extended to handle new file types or database integrations.

### Key Features

- **Real-Time Monitoring**: Uses `watchdog` to monitor a designated folder for new files.
- **File Format Support**: Processes both CSV and Excel (`.xls`, `.xlsx`) file formats.
- **Database Support**: Modular handlers for MongoDB, PostgreSQL, and Snowflake databases.
- **Polling-Based Detection**: Ensures the stability of detected files before processing.
- **Extensible Architecture**: Easily integrates additional file formats or database types by implementing abstract handler classes.
- **Asynchronous Processing**: Utilizes `asyncio` for non-blocking and concurrent file processing.
- **Logging**: Centralized logging to monitor application events.
- **Validation**: Uses `pydantic` models to ensure data validation before inserting into databases.

---

## Code-Based Architecture

### Folder Structure

```text
.
├── src/
│   ├── handlers/
│   │   ├── file_fetch_handlers/
│   │   │   ├── csv_fetch_handler.py       # CSV file processing logic
│   │   │   ├── excel_fetch_handler.py     # Excel file processing logic
│   │   │   └── base_file_fetch_handler.py # Abstract class/interface for file handlers
│   │   ├── db_handlers/
│   │   │   ├── mongo_handler.py           # MongoDB interaction logic
│   │   │   ├── postgres_handler.py        # PostgreSQL interaction logic
│   │   │   ├── snowflake_handler.py       # Snowflake interaction logic
│   │   │   └── base_db_handler.py         # Abstract class/interface for database handlers
│   │   └── files_monitor.py               # Folder monitoring logic
│   ├── main.py                            # Main entry point to run the tool
│   ├── models/
│   │   └── data_record.py                 # Pydantic models for validating CSV and Excel records
│   └── utils/
│       └── logger.py                      # Centralized logging utility
```

### Core Components

1. **File Fetch Handlers (`src/handlers/file_fetch_handlers`)**:
    - `base_file_fetch_handler.py`: Defines the abstract base class `BaseFileFetchHandler` that all file handlers must implement. It includes logic for reading files, checking file stability, calculating checksums, and validating data.
    - `csv_fetch_handler.py`: Implements `BaseFileFetchHandler` for CSV files.
    - `excel_fetch_handler.py`: Implements `BaseFileFetchHandler` for Excel files.

2. **Database Handlers (`src/handlers/db_handlers`)**:
    - `base_db_handler.py`: Abstract class that defines the interface for database operations. Concrete classes for MongoDB, PostgreSQL, and Snowflake inherit from this class.
    - `mongo_handler.py`: Handles MongoDB interactions.
    - `postgres_handler.py`: Handles PostgreSQL interactions.
    - `snowflake_handler.py`: Handles Snowflake interactions.

3. **Folder Monitoring (`src/handlers/files_monitor.py`)**:
    - Contains the logic for monitoring the folder in real-time using `watchdog`. It detects file creation, determines the file type, and assigns the appropriate handler to process the file.

4. **Logging Utility (`src/utils/logger.py`)**:
    - A centralized logger used to log various events, including file detection, processing results, and error handling.

5. **Models (`src/models/data_record.py`)**:
    - Defines `pydantic` models for validating CSV and Excel data records.

### Design Patterns

- **Strategy Pattern**: Applied in the file fetch handlers and database handlers to provide different implementations for various file formats and databases.
- **Template Method Pattern**: Utilized in the `BaseFileFetchHandler` class, which provides a template method for processing files. Subclasses (e.g., `CSVFetchHandler`, `ExcelFetchHandler`) supply specific implementation details.

### Event Loop Management with AsyncIO

#### Overview

The project uses `asyncio` to manage asynchronous file monitoring and processing, ensuring that the event loop responsible for handling file events runs consistently in the main thread. This design prevents blocking the system while waiting for file events and avoids `asyncio` runtime issues.

#### Key Components

1. **Main Event Loop Initialization**:
    - In `main.py`, the main event loop is initialized using `asyncio.run()` to execute the `run_monitoring()` function, which starts the folder monitoring process.

    ```python
    if __name__ == "__main__":
        asyncio.run(run_monitoring())
    ```

2. **Passing the Event Loop**:
    - The event loop is explicitly passed to the `FolderMonitor` and `FileMonitorHandler` classes during initialization. This ensures that file events handled by `watchdog` can safely submit tasks to the main event loop.

    ```python
    folder_monitor = FolderMonitor(
        folder_to_monitor=TEST_FOLDER,
        poll_interval=2,
        db_handler=db_handler,
        dry_run=True,
        file_handlers=file_handlers,
        loop=asyncio.get_running_loop()  # Pass the event loop
    )
    ```

3. **Running Coroutines from Different Threads**:
    - When a file is detected by `watchdog` (which runs in a separate thread), the file processing task is submitted to the main event loop using `asyncio.run_coroutine_threadsafe()`, ensuring thread-safe execution.

    ```python
    asyncio.run_coroutine_threadsafe(self.handle_file(file_path), self.loop)
    ```

4. **Asynchronous File Monitoring**:
    - The `start_monitoring()` method in `FolderMonitor` is an asynchronous method (`async def`) that uses `await asyncio.sleep()` to avoid blocking the event loop.

    ```python
    async def start_monitoring(self) -> None:
        self.logger.info(f"Monitoring folder: {self.folder_to_monitor}")
        event_handler = FileMonitorHandler(self.db_handler, dry_run=self.dry_run,
                                           file_handlers=self.file_handlers, loop=self.loop)
        observer = Observer()
        observer.schedule(event_handler, self.folder_to_monitor, recursive=False)
        observer.start()

        try:
            while True:
                await asyncio.sleep(self.poll_interval)  # Non-blocking sleep
        except KeyboardInterrupt:
            self.logger.info("Shutting down folder monitoring...")
            observer.stop()
        observer.join()
    ```

#### Why the Event Loop Design Matters

- **Single Event Loop**: Ensures that all asynchronous tasks are managed within a single event loop running in the main thread, preventing `RuntimeError` related to multiple event loops.
- **Thread-Safe Execution**: By using `asyncio.run_coroutine_threadsafe()`, asynchronous tasks are safely submitted from different threads.
- **Non-Blocking Operations**: Utilizing `await asyncio.sleep()` maintains the non-blocking nature of the event loop, allowing concurrent handling of multiple file events.

---

## Getting Started

### Prerequisites

- **Python 3.9+**
- Install the required libraries via `pip`:

  ```bash
  pip install pandas watchdog openpyxl psycopg2 snowflake-connector-python pymongo python-dotenv
  ```

### Setup

1. **Clone the Repository**:

    ```bash
    git clone https://github.com/PenHsuanWang/file-data-fetcher.git
    ```

2. **Navigate to the Project Directory**:

    ```bash
    cd file-data-fetcher
    ```

3. **Install Dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

4. **Create and Configure the `.env` File**:

    Create a `.env` file to store your configuration:

    ```bash
    touch .env
    ```

    Update the `.env` file with your database credentials and folder settings:

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

    Ensure that `.env` is Git-ignored by confirming its presence in the `.gitignore` file.

5. **Load Environment Variables**:

    In `config/settings.py`, ensure environment variables are loaded using `python-dotenv`:

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

### Setting Up Your Own File Monitoring Flow

The `src/main.py` file provides a comprehensive example of setting up the file monitoring system using the project's modules and handlers. Follow these steps to customize and extend the monitoring flow:

#### Step 1: Create the Folder to Monitor

Ensure the folder you want to monitor exists. This is where new CSV and Excel files will be placed for processing.

```bash
mkdir -p /path/to/your/monitoring/folder
```

In `main.py`, reference this folder as `TEST_FOLDER`:

```python
# Define the folder to be monitored
TEST_FOLDER = "/path/to/your/monitoring/folder"
```

You can hard-code the folder path or load it from an environment variable or configuration file (such as `.env`).

#### Step 2: Configure Your Database Handler

Choose the appropriate database handler based on your target database. In `main.py`:

```python
# Select MongoDB as the database handler for demonstration purposes
db_handler = MongoDBHandler()

# For PostgreSQL, use:
# db_handler = PostgresHandler()

# For Snowflake, use:
# db_handler = SnowflakeHandler()
```

To support additional databases, create a new handler by inheriting from `BaseDBHandler` located in `src/handlers/db_handlers/base_db_handler.py` and implement the required methods (`save_data`, etc.).

Ensure your database credentials are set in your `.env` file as described in the "Setup" section.

#### Step 3: Customize File Handlers

By default, the project supports CSV and Excel files. To customize or add support for new file formats, create new file handlers.

To use existing handlers:

```python
# Set up the file handlers for CSV and Excel files
file_handlers = {
    '.csv': CSVFetchHandler,
    '.xls': ExcelFetchHandler,
    '.xlsx': ExcelFetchHandler
}
```

To add support for additional file formats, such as JSON:

```python
# Add support for JSON files
file_handlers = {
    '.csv': CSVFetchHandler,
    '.xls': ExcelFetchHandler,
    '.xlsx': ExcelFetchHandler,
    '.json': JSONFetchHandler  # New handler for JSON files
}
```

The `JSONFetchHandler` class should inherit from `BaseFileFetchHandler` and implement the `read_file` method.

#### Step 4: Set Up the Folder Monitor

Initialize the `FolderMonitor` in `main.py` with the necessary parameters:

```python
folder_monitor = FolderMonitor(
    folder_to_monitor=TEST_FOLDER,
    poll_interval=2,  # Polling interval in seconds
    db_handler=db_handler,
    dry_run=True,  # Set to False to insert data into the database
    file_handlers=file_handlers,
    loop=asyncio.get_running_loop()  # Pass the asyncio event loop
)
```

Adjust the `poll_interval` to control how often the system checks for file changes and set `dry_run` to `False` to enable actual database insertions.

#### Step 5: Run the Monitoring System

Start monitoring by executing `main.py`:

```bash
python src/main.py
```

This runs the monitoring process in an asynchronous loop, continuously checking for new files and processing them upon detection.

Sample log output when files are detected and processed:

```text
2024-10-03 18:34:00,726 - app_logger - INFO - File event detected: /path/to/your/monitoring/folder/sample_1727951640.csv
2024-10-03 18:34:02,731 - app_logger - INFO - File /path/to/your/monitoring/folder/sample_1727951640.csv is ready. Processing it...
2024-10-03 18:34:02,733 - app_logger - INFO - Attempting to handle the file /path/to/your/monitoring/folder/sample_1727951640.csv
2024-10-03 18:34:02,733 - app_logger - INFO - Reading content of CSV file: /path/to/your/monitoring/folder/sample_1727951640.csv
2024-10-03 18:34:02,744 - app_logger - INFO - File content:
   name  age          city
  Alice   25      New York
    Bob   30 San Francisco
Charlie   35   Los Angeles
```

#### Step 6: Test the Setup with Sample Files

Use the `src/example/create_file.py` script to generate sample CSV or Excel files in the monitored folder for testing:

```bash
python src/example/create_file.py
```

You will be prompted to choose between creating a CSV or Excel file. The file will be generated in the folder specified in `TEST_FOLDER`.

Example output:

```text
Sample CSV file created at: ./test_monitor_folder/sample_1234567890.csv
```

#### Step 7: Extend the System

- **Add Support for New File Formats**: Create a new handler by inheriting from `BaseFileFetchHandler` and implementing the logic for reading and processing the new file type.
  
- **Support Additional Databases**: Add new database handlers by inheriting from `BaseDBHandler` and implementing the `save_data` method.
  
- **Change Polling Interval**: Adjust the `poll_interval` parameter to control how frequently the folder is checked for file updates.

---

## Customization

### File Handlers

Extend functionality by adding new file handlers. Implement a new handler by inheriting from the `BaseFileFetchHandler` interface located in `src/handlers/file_fetch_handlers/base_file_fetch_handler.py`.

### Database Handlers

To support additional databases (e.g., MySQL), create new handlers by inheriting from `BaseDBHandler` located in `src/handlers/db_handlers/base_db_handler.py` and implementing the required methods.

---

## Testing

Run the unit tests to ensure everything works as expected:

```bash
pytest --cov tests
```

---

## Dependencies

- **pandas**: Data manipulation for CSV/Excel files.
- **watchdog**: Real-time folder monitoring.
- **pymongo**: MongoDB integration.
- **psycopg2**: PostgreSQL database interaction.
- **snowflake-connector-python**: Snowflake database interaction.
- **openpyxl**: Excel file processing.
- **python-dotenv**: Loading environment variables from `.env` files.

For a full list of dependencies, refer to the `requirements.txt` file.

