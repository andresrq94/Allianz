Data Loader for Allianz Project
This project is designed to load customer and product data from a CSV file into a SQL Server database. The script processes the data in chunks, allowing for efficient memory usage, and includes functionalities such as data validation, encryption, and logging.
  
Required Python packages:
pandas
sqlalchemy
cryptography
pyodbc

You can install the required packages using pip



Make sure you have a SQL Server instance running and accessible.
Prepare your CSV file with the required format.

Configuration
The script uses a JSON configuration file to manage database connection details and encryption settings.

JSON Configuration File Structure
Create a JSON file named config_file.json with the following structure:

{
    "database": {
        "server": "yourserver",
        "database": "your_db",
        "user": "your_user",
        "password": "your_password",
        "driver": "your_password" #"ODBC Driver 18 for SQL Server"
    },
    "file": {
        "path": "the_path_of_csv",
        "chunksize": 1000  // Optional: specify the size of the data chunks to load
    },
    "encryption": {
        "key": "created_in_code",
        "encrypt": false  // Set to true to enable encryption
    }
}

database: Contains connection details for your SQL Server database.
file: Specifies the path to the CSV file and the optional chunksize parameter.
encryption: Controls whether data should be encrypted during processing and stores the encryption key.

Code Overview
The main functionalities of the script are organized into several functions:

load_config: Loads the JSON configuration file and checks for required keys.

load_data: Loads data from the specified CSV file in chunks, converting column names to lowercase and string values to uppercase.

validate_data: Validates data for missing values and outliers.
encrypt_data: Encrypts sensitive data if encryption is enabled in the configuration.

filter_existing_data: Filters out rows from the DataFrame that already exist in the database based on a composite key.

extract_customer_dimension: Extracts unique customer data for the Customer Dimension table.

extract_product_dimension: Extracts unique product data for the Product Dimension table.

create_sales_df: Creates a sales DataFrame that includes customer IDs and product IDs.

upload_data: Uploads the sales data to the SQL Server database.

upload_dimension: Uploads customer and product dimension data to the SQL Server database.

main: The main function that orchestrates the loading, processing, and uploading of data.

The JSON configuration is crucial for setting up the environment. Below is a detailed explanation of each section:

database
server: The SQL Server instance to connect to (e.g., localhost\\SQLEXPRESS).
database: The name of the database where data will be uploaded.
user: The username for authentication.
password: The password for authentication.
driver: The ODBC driver used to connect to SQL Server.

file
path: The full path to the CSV file containing the data to be loaded.
chunksize: The size of the data chunks to process at a time (optional, defaults to 1000).

encryption
key: The encryption key used to encrypt sensitive data. This is only necessary if encryption is enabled.
encrypt: A boolean value indicating whether to encrypt sensitive data during processing.

Logging
The script uses Python's built-in logging module to log events at various levels (INFO, WARNING, ERROR). This provides visibility into the data loading process and helps in troubleshooting.
