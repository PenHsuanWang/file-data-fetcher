# src/create_file.py

import os
import pandas as pd
import time

# Define the folder where files will be created
TEST_FOLDER = "./test_monitor_folder"


def setup_test_environment():
    """
    Sets up the test environment by creating the test folder if it doesn't exist.
    """
    os.makedirs(TEST_FOLDER, exist_ok=True)
    print(f"Test environment set up. Folder created at: {TEST_FOLDER}")


def create_csv_file():
    """
    Creates a sample CSV file in the monitored folder.
    """
    csv_data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "city": ["New York", "San Francisco", "Los Angeles"]
    }
    df_csv = pd.DataFrame(csv_data)
    timestamp = int(time.time())
    csv_file_path = os.path.join(TEST_FOLDER, f"sample_{timestamp}.csv")
    df_csv.to_csv(csv_file_path, index=False)
    print(f"Sample CSV file created at: {csv_file_path}")


def create_excel_file():
    """
    Creates a sample Excel file in the monitored folder.
    """
    excel_data = {
        "product": ["A", "B", "C"],
        "price": [100, 200, 300],
        "quantity": [10, 5, 2]
    }
    df_excel = pd.DataFrame(excel_data)
    timestamp = int(time.time())
    excel_file_path = os.path.join(TEST_FOLDER, f"sample_{timestamp}.xlsx")
    df_excel.to_excel(excel_file_path, index=False)
    print(f"Sample Excel file created at: {excel_file_path}")


def main():
    """
    Main function to set up the environment and create files in the monitored folder.
    """
    # Step 1: Set up the test environment (create folder)
    setup_test_environment()

    # Step 2: Prompt the user to create a file (CSV or Excel)
    print("Choose the type of file to create:")
    print("1. CSV File")
    print("2. Excel File")
    choice = input("Enter choice (1 or 2): ")

    if choice == "1":
        create_csv_file()
    elif choice == "2":
        create_excel_file()
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()