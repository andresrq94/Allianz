import pandas as pd
import sqlalchemy

# Path to your CSV file
ruta = 'C:/Users/AndresRoldan/Desktop/Andres/Allianz/allianz_db.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(ruta)

# Rename 'timestamp' to 'sale_date'
df = df.rename(columns={'timestamp': 'sale_date'})

#Change sale_date column format to date
df['sale_date'] = pd.to_datetime(df['sale_date'], format='%m/%d/%Y %H:%M')


# Connection details
servidor = 'localhost\\SQLEXPRESS'
base_datos = 'Allianz'
usuario = 'user_andres'
contrasena = '123456'
driver = 'ODBC Driver 18 for SQL Server'

# Adjusted connection string
connection_string = f'mssql+pyodbc://{usuario}:{contrasena}@{servidor}/{base_datos}?driver={driver}&TrustServerCertificate=yes'
engine = sqlalchemy.create_engine(connection_string)

# Upload DataFrame to SQL Server
df.to_sql(name='sales', con=engine, if_exists='replace', index=False)
print("Data uploaded successfully.")
