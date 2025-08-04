import os
import pandas as pd
from sqlalchemy import create_engine
import argparse

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_file = params.csv_file

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    first_chunk = next(df_iter)
    first_chunk["pickup_datetime"] = pd.to_datetime(first_chunk["pickup_datetime"])
    first_chunk["dropoff_datetime"] = pd.to_datetime(first_chunk["dropoff_datetime"])

    first_chunk.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

    first_chunk.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False
    )

    for i, chunk in enumerate(df_iter, start=2):
        chunk["pickup_datetime"] = pd.to_datetime(chunk["pickup_datetime"])
        chunk["dropoff_datetime"] = pd.to_datetime(chunk["dropoff_datetime"])
        
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )
        
        print(f"Inserted chunk {i}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="ingest CSV data to Postgres")

    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--db")
    parser.add_argument("--table_name")
    parser.add_argument("--csv_file")


    args = parser.parse_args()
    
    main(args)