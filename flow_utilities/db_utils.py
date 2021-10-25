"""
Using local DB in docker:
docker run -d --name demo_postgres -v dbdata:/var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD=xyz postgres:11
"""
import pandas as pd
from prefect.client import Secret
from sqlalchemy import create_engine


def get_db_connection_string() -> str:
    user = Secret("POSTGRES_USER").get()
    pwd = Secret("POSTGRES_PASS").get()
    return f"postgresql://{user}:{pwd}@localhost:5432/postgres"


def get_df_from_sql_query(table_or_query: str) -> pd.DataFrame:
    db = get_db_connection_string()
    engine = create_engine(db)
    return pd.read_sql(table_or_query, engine)


def load_df_to_db(df: pd.DataFrame, table_name: str, schema: str = "jaffle_shop") -> None:
    conn_string = get_db_connection_string()
    db_engine = create_engine(conn_string)
    conn = db_engine.connect()
    conn.execute(f"TRUNCATE TABLE {schema}.{table_name};")
    df.to_sql(table_name, schema=schema, con=db_engine, if_exists="append")
    conn.close()
