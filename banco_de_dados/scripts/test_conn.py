import pyodbc
import pandas as pd

# Configuração da conexão
DRIVER = "ODBC Driver 17 for SQL Server"
SERVER = r"(localdb)\dev"
DATABASE = "PBS_PROCFIT_DADOS"

CONN_STR = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# Testando conexão e listando tabelas
with pyodbc.connect(CONN_STR) as conn:
    query = "SELECT TOP 10 name, create_date FROM sys.tables ORDER BY create_date DESC"
    df = pd.read_sql(query, conn)

print("Conexão bem-sucedida!")
print(df)
