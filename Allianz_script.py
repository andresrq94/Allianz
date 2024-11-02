
import pandas as pd

from sqlalchemy import create_engine
# Especifica la ruta del archivo y el delimitador
ruta = 'C:/Users/AndresRoldan/Downloads/allianz_db.csv'

# Lee el archivo delimitado en un DataFrame
df = pd.read_csv(ruta)

# Configura la cadena de conexión
servidor = 'DESKTOP-RRLTHKB\SQLEXPRESS'  # Nombre del servidor SQL Server Express
base_datos = 'Allianz'     # Cambia por el nombre de tu base de datos
usuario = 'usuario_sql'              # Usuario de SQL Server (si usas autenticación SQL)
contrasena = 'contraseña_sql'        # Contraseña del usuario
driver = 'ODBC Driver 17 for SQL Server'

# Crear el motor de conexión
cadena_conexion = f'mssql+pyodbc://{usuario}:{contrasena}@{servidor}/{base_datos}?driver={driver}'
engine = create_engine(cadena_conexion)


df.to_sql(name = 'sales', engine, if_exists='replace', index=False)
print("Datos cargados exitosamente.")