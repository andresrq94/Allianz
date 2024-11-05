import pandas as pd
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
import logging
import json
from cryptography.fernet import Fernet
import os
import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define your composite key columns
#COMPOSITE_KEY_COLUMNS = ['customer_id', 'product_id', 'sale_date', 'quantity']
last_transaction_id = 0
last_customer_id = 0
last_product_id = 0

def load_config(config_path):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        required_keys = ['file', 'database', 'encryption']
        for key in required_keys:
            if key not in config:
                logging.error(f"Missing required configuration key: {key}")
                raise ValueError(f"Missing required configuration key: {key}")
        logging.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON configuration: {config_path}")
        raise


def load_data(path, chunksize=1000):
    """Load data from CSV file into a DataFrame in chunks, with headers in lowercase and string values in uppercase."""
    try:
        for chunk in pd.read_csv(path, chunksize=chunksize):
            # Convert all column names to lowercase
            chunk.columns = [col.lower().replace(" ", "_") for col in chunk.columns]

            # Convert all string values to uppercase
            for col in chunk.select_dtypes(include=['object']).columns:
                chunk[col] = chunk[col].str.upper()

            yield chunk  # Yield the modified chunk
        logging.info("Data loaded and processed successfully.")
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
        raise
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def save_config(config_path, config):
    """Save the updated configuration back to the JSON file."""
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        logging.info("Configuration file updated with new encryption key.")
    except Exception as e:
        logging.error(f"Error saving updated configuration: {e}")
        raise

def validate_data(df):
    """Validate data for missing values, outliers, and inconsistencies."""
    # Get the current year
    current_year = datetime.datetime.now().year
    if df.isnull().any().any():
        logging.warning("Missing values found in the data.")
        df.fillna({'income_range': 'Middle Earner', 'year_of_birth': df['year_of_birth'].mean()}, inplace=True)

    # Calculate age from year_of_birth and check for age range constraints
    df['age'] = current_year - df['year_of_birth']
    if not df['age'].between(17, 100).all():
        logging.warning("Outliers detected in 'year_of_birth'.")
        df = df[df['age'].between(17, 100)]
    if (df['premium'] < 0).any():
        logging.warning("Negative Premium values found.")
        df = df[df['premium'] >= 0]
    return df

def encrypt_data(df, cipher, encrypt):
    """Encrypt sensitive data if encryption is enabled in the config."""
    if encrypt:
        logging.info("Encryption enabled: encrypting sensitive data.")
        if 'personal_id' in df.columns:
            df['personal_id'] = df['personal_id'].apply(lambda x: cipher.encrypt(x.encode()).decode())
    else:
        logging.info("Encryption disabled: skipping encryption step.")
    return df

def filter_existing_data(df, connection, table_name):
    """Filter out rows from the DataFrame that already exist in the database based on composite key."""
    try:
        inspector = inspect(connection)
        if not inspector.has_table(table_name):
            logging.info(f"Table '{table_name}' does not exist. Skipping filtering step.")
            return df

        # Get the existing composite key values from the database
        if table_name == 'dim_customer':
            composite_key_columns = ['merge_key']
        elif table_name == 'dim_product':
            composite_key_columns = ['merge_key']
        elif table_name == 'sales':
            composite_key_columns = ['customer_id', 'product_id', 'sale_date', 'quantity']
        else:
            logging.error(f"Unknown table name: {table_name}")
            raise ValueError(f"Unknown table name: {table_name}")

        query = f"SELECT {', '.join(composite_key_columns)} FROM {table_name}"
        existing_keys = pd.read_sql(query, connection)

        # Create a composite key column in both existing data and incoming data
        existing_keys['composite_key'] = existing_keys[composite_key_columns].astype(str).agg('-'.join, axis=1)
        df['composite_key'] = df[composite_key_columns].astype(str).agg('-'.join, axis=1)

        # Filter the DataFrame to include only new rows
        df_filtered = df[~df['composite_key'].isin(existing_keys['composite_key'])]

        # Drop the composite key column after filtering
        df_filtered = df_filtered.drop(columns=['composite_key'])
        return df_filtered
    except Exception as e:
        logging.error(f"Error filtering existing data: {e}")
        raise


def extract_customer_dimension(df):
    """Extract unique customer data for the Customer Dimension table."""
    customer_cols = ['personal_id', 'name', 'country', 'year_of_birth', 'income_range']
    customer_df = df[customer_cols].drop_duplicates().reset_index(drop=True)

    global last_customer_id  # Access the global variable

    # Create a unique transaction_id that continues from the last transaction_id
    customer_df['customer_id'] = range(last_customer_id + 1, last_customer_id + len(customer_df) + 1)

    customer_df = customer_df[['customer_id', 'personal_id', 'name', 'country','year_of_birth','income_range']]

    return customer_df


def extract_product_dimension(df):
    """Extract unique product data for the Product Dimension table."""
    product_cols = ['company', 'product', 'premium']
    product_df = df[product_cols].drop_duplicates().reset_index(drop=True)

    global last_product_id  # Access the global variable

    # Create a unique transaction_id that continues from the last transaction_id
    product_df['product_id'] = range(last_product_id + 1, last_product_id + len(product_df) + 1)

    product_df = product_df[['product_id','company', 'product', 'premium']]
    return product_df


def create_sales_df(df, customer_df, product_df):
    """Create a Sales DataFrame with timestamp, product_id, customer_id, and quantity."""
    # Log the columns present in the original DataFrame
    #logging.info(f"Original DataFrame columns: {df.columns.tolist()}")
    global last_transaction_id  # Access the global variable

    # Check if required columns exist
    required_columns = ['timestamp', 'quantity', 'company', 'product', 'premium', 'personal_id', 'name', 'country', 'year_of_birth', 'income_range']
    for col in required_columns:
        if col not in df.columns:
            logging.error(f"Missing column in DataFrame: {col}")
            raise KeyError(f"Missing column in DataFrame: {col}")

    # Prepare the sales DataFrame
    sales_df = df[required_columns].copy()
    sales_df.rename(columns={'timestamp': 'sale_date'}, inplace=True)

    # Create a unique key for merging products
    product_df['merge_key'] = product_df['company'] + product_df['product'] + product_df['premium'].astype(str)
    sales_df['product_merge_key'] = sales_df['company'] + sales_df['product'] + sales_df['premium'].astype(str)

    # Merge to get product_id
    sales_df = sales_df.merge(product_df[['merge_key', 'product_id']], how='left',
                              left_on=['product_merge_key'], right_on=['merge_key'])

    # Log after merging product_id
    if 'product_id' not in sales_df.columns:
        logging.warning("product_id not found in sales_df after merging with product_df.")

    # Create a unique key for merging customers
    customer_df['merge_key'] = customer_df['personal_id'] + customer_df['name'] + customer_df['country'] + customer_df['year_of_birth'].astype(str) + customer_df['income_range']
    sales_df['customer_merge_key'] = sales_df['personal_id'] + sales_df['name'] + sales_df['country'] + sales_df[
        'year_of_birth'].astype(str) + sales_df['income_range']

    # Merge to get customer_id
    sales_df = sales_df.merge(customer_df[['merge_key', 'customer_id']], how='left',
                              left_on=['customer_merge_key'],
                              right_on=['merge_key'])

    # Log after merging customer_id
    if 'customer_id' not in sales_df.columns:
        logging.warning("customer_id not found in sales_df after merging with customer_df.")

    # Keep only the necessary columns in the final sales DataFrame
    # Create a unique transaction_id that continues from the last transaction_id
    sales_df['transaction_id'] = range(last_transaction_id + 1, last_transaction_id + len(sales_df) + 1)

    # Update the global transaction_id counter
    last_transaction_id += len(sales_df)

    sales_df = sales_df[['transaction_id','customer_id', 'product_id', 'quantity','sale_date']]

    # Replace all double spaces with underscores in the 'product' and 'name' columns
    product_df['product'] = product_df['product'].str.replace(" ", "_")
    customer_df['name'] = customer_df['name'].str.replace(" ", "_")

    product_split = product_df['product'].str.split('|', expand=True)

    # Check how many columns were created
    if product_split.shape[1] == 2:
        product_df[['product_category', 'product_detail']] = product_split
    else:
        logging.warning(f"Unexpected number of columns when splitting 'product'. Found: {product_split.shape[1]}")

    # Split the 'name' column in customer_df, removing spaces around the delimiter only
    customer_split = customer_df['name'].str.split('//', expand=True)

    # Check how many columns were created
    if customer_split.shape[1] == 2:
        customer_df[['first_name', 'last_name']] = customer_split
    else:
        logging.warning(f"Unexpected number of columns when splitting 'name'. Found: {customer_split.shape[1]}")

    # Drop the merge_key columns after merging
    product_df.drop(columns=['product'], inplace=True, errors='ignore')
    customer_df.drop(columns=['name'], inplace=True, errors='ignore')

    return sales_df

def upload_data(df, connection_string, table_name='sales', chunk_size=1000, schema='dbo'):
    """Upload DataFrame to SQL Server, only uploading new rows."""
    try:
        engine = sqlalchemy.create_engine(connection_string)  # Set echo=True for SQLAlchemy logging
        with engine.connect() as connection:
            # Filter out rows that already exist in the database based on the composite key
            df_to_upload = filter_existing_data(df, connection, table_name)

            if not df_to_upload.empty:
                # Explicitly specify the schema when uploading data
                df_to_upload.to_sql(name=table_name, con=connection, schema=schema,
                                    if_exists='append', index=False, chunksize=chunk_size)
                logging.info(f"{len(df_to_upload)} new rows uploaded successfully.")
            else:
                logging.info("No new rows to upload.")

            # Explicitly commit if the database or driver requires it
            connection.commit()
    except SQLAlchemyError as e:
        logging.error(f"Error uploading data to SQL Server: {e}")
        raise

def save_to_csv(path, connection_string, table_name):
    """Save a DataFrame from a SQL table to a CSV file, replacing existing content."""
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection_string)
    df.to_csv(path, index=False)
    logging.info(f"{table_name} data saved to CSV at {path}.")

def upload_dimension(df, connection_string, table_name, schema='dbo'):
    """Upload DataFrame to SQL Server, only uploading new rows."""
    try:
        engine = sqlalchemy.create_engine(connection_string)  # Set echo=True for SQLAlchemy logging
        with engine.connect() as connection:
            # Filter out rows that already exist in the database based on the composite key
            df_to_upload = filter_existing_data(df, connection, table_name)

            if not df_to_upload.empty:
                # Explicitly specify the schema when uploading data
                df_to_upload.to_sql(name=table_name, con=connection, schema=schema,
                                    if_exists='append', index=False)
                logging.info(f"{len(df_to_upload)} new rows uploaded successfully.")
            else:
                logging.info("No new rows to upload.")

            # Explicitly commit if the database or driver requires it
            connection.commit()
    except SQLAlchemyError as e:
        logging.error(f"Error uploading data to SQL Server: {e}")
        raise


def main():
    # Path to your JSON config file
    config_path = 'C:/Users/AndresRoldan/Desktop/Andres/Allianz/python_assignment/config/config_file.json'

    # Load configuration
    config = load_config(config_path)
    config_chunk_size = config['file']['chunksize']
    # Check encryption settings and potentially generate a new key
    encrypt = config['encryption'].get('encrypt', False)

    if encrypt:
        encryption_key = Fernet.generate_key().decode()
        config['encryption']['key'] = encryption_key
        save_config(config_path, config)
        cipher = Fernet(encryption_key.encode())
    else:
        encryption_key = config['encryption']['key'].encode()
        cipher = Fernet(encryption_key)

    connection_string = (
        f"mssql+pyodbc://{config['database']['user']}:{config['database']['password']}@"
        f"{config['database']['server']}/{config['database']['database']}?driver={config['database']['driver']}&TrustServerCertificate=yes"
    )

    # Define the output CSV file path
    output_base_path = config['file']['output_path']

    try:
        for chunk in load_data(config['file']['path'], chunksize = config_chunk_size):
            chunk = validate_data(chunk)
            chunk = encrypt_data(chunk, cipher, encrypt)

            # Extract customer and product dimensions
            customer_df = extract_customer_dimension(chunk)
            product_df = extract_product_dimension(chunk)

            # Create the sales DataFrame
            sales_df = create_sales_df(chunk, customer_df, product_df)

            # Upload data to the respective tables
            upload_dimension(customer_df, connection_string, table_name='dim_customer')
            upload_dimension(product_df, connection_string, table_name='dim_product')
            upload_data(sales_df, connection_string, table_name='sales')

            product_df.drop(columns=['composite_key'], inplace=True, errors='ignore')
            customer_df.drop(columns=['composite_key'], inplace=True, errors='ignore')
            sales_df.drop(columns=['composite_key'], inplace=True, errors='ignore')

        try:
            # Save customer, product, and sales data to CSV
            save_to_csv(os.path.join(output_base_path, 'dim_customer.csv'), connection_string,
                        table_name='dim_customer')
            save_to_csv(os.path.join(output_base_path, 'dim_product.csv'), connection_string, table_name='dim_product')
            save_to_csv(os.path.join(output_base_path, 'sales.csv'), connection_string, table_name='sales')

        except Exception as e:
            logging.error(f"An error occurred: {e}")

    except Exception as e:
        logging.error(f"An error occurred in the main process: {e}")


if __name__ == "__main__":
    main()
