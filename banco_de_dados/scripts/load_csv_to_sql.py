import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, DateTime, String, Boolean

# ---- CONFIG DA CONEXÃO ----
DRIVER = "ODBC Driver 17 for SQL Server"   # use 18 se tiver
SERVER = r"(localdb)\dev"
DATABASE = "PBS_PROCFIT_DADOS"

def make_engine():
    params = f"driver={DRIVER.replace(' ', '+')}&trusted_connection=yes&TrustServerCertificate=yes"
    url = f"mssql+pyodbc://@{SERVER}/{DATABASE}?{params}"
    # fast_executemany acelera inserts em massa
    return create_engine(url, fast_executemany=True)

# Mapeamento simples de dtypes pandas -> SQL
def infer_sql_types(df: pd.DataFrame):
    sql_types = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            sql_types[col] = Integer()
        elif pd.api.types.is_float_dtype(dtype):
            sql_types[col] = Float()
        elif pd.api.types.is_bool_dtype(dtype):
            sql_types[col] = Boolean()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_types[col] = DateTime()
        else:
            # fallback seguro: NVARCHAR(4000)
            sql_types[col] = String(length=4000)
    return sql_types

def main():
    ap = argparse.ArgumentParser(description="Carrega CSV para tabela no SQL Server (LocalDB).")
    ap.add_argument("--csv", required=True, help="Caminho do arquivo CSV.")
    ap.add_argument("--table", required=True, help="Nome da tabela de destino (sem schema).")
    ap.add_argument("--schema", default="dbo", help="Schema (padrão: dbo).")
    ap.add_argument("--if-exists", default="append", choices=["append", "replace", "fail"],
                    help="Comportamento se a tabela existir.")
    ap.add_argument("--sep", default=",", help="Separador do CSV (padrão ,).")
    ap.add_argument("--encoding", default="utf-8", help="Encoding do CSV.")
    ap.add_argument("--chunksize", type=int, default=5000, help="Tamanho do lote para insert.")
    args = ap.parse_args()

    print(f"Lendo CSV: {args.csv}")
    df = pd.read_csv(args.csv, sep=args.sep, encoding=args.encoding)

    # tenta converter colunas que parecem data
    for c in df.columns:
        try:
            df[c] = pd.to_datetime(df[c], errors="raise")
        except Exception:
            pass  # mantém como está

    engine = make_engine()
    sql_types = infer_sql_types(df)

    print(f"Inserindo em {args.schema}.{args.table} ({len(df)} linhas)...")
    with engine.begin() as con:
        df.to_sql(
            name=args.table,
            con=con,
            schema=args.schema,
            if_exists=args.if_exists,
            index=False,
            dtype=sql_types,
            method="multi",
            chunksize=args.chunksize,
        )
        # checagem rápida
        rows = con.execute(
            text(f"SELECT COUNT(*) FROM {args.schema}.{args.table}")
        ).scalar()
        print(f"OK! Linhas na tabela: {rows}")

if __name__ == "__main__":
    main()
