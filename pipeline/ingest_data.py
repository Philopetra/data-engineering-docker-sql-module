#!/usr/bin/env python
# coding: utf-8

import os
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# --- Configuration for Yellow Taxi (CSV) ---
YELLOW_DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

YELLOW_PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# --- Configuration for Taxi Zones (CSV) ---
ZONES_DTYPE = {
    "LocationID": "Int64", 
    "service_zone": "string",
    "Zone": "string",
    "Borough": "string",
}

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data (for trips)')
@click.option('--month', default=11, type=int, help='Month of the data (for trips)')
@click.option('--target-table', default='', help='Target table name (defaults based on service if empty)')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option('--service', type=click.Choice(['yellow', 'green', 'zones']), default='yellow', help='Service type to ingest')



def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize, service):
    """Ingest NYC taxi data (Yellow, Green, or Zones) into PostgreSQL database."""
    
    # 1. Setup Database Connection
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    print(f"Connected to {pg_db} at {pg_host}")

    # 2. Determine URL, Table Name, and Ingestion Strategy
    if service == 'yellow':
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz'
        table_name = target_table if target_table else f'yellow_tripdata_{year}_{month:02d}'
        print(f"Processing Yellow Taxi Data: {url}")
        ingest_csv_iterative(url, table_name, engine, chunksize, YELLOW_DTYPE, YELLOW_PARSE_DATES)

    elif service == 'zones':
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
        table_name = target_table if target_table else 'taxi_zone_lookup'
        print(f"Processing Taxi Zones: {url}")
        ingest_csv_full(url, table_name, engine, ZONES_DTYPE)

    elif service == 'green':
        # Parquet files are typically smaller and self-describing, so we load them fully.
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
        table_name = target_table if target_table else f'green_tripdata_{year}_{month:02d}'
        print(f"Processing Green Taxi Data (Parquet): {url}")
        ingest_parquet(url, table_name, engine)


def ingest_csv_iterative(url, table_name, engine, chunksize, dtype, parse_dates):
    """Handles large CSVs by reading in chunks."""
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
        low_memory=False
    )

    first = True
    
    # We use a loop to process chunks, showing a progress bar isn't exact without total length, 
    # but we can wrap the iterator to show activity.
    for df_chunk in tqdm(df_iter, desc=f"Ingesting {table_name}"):
        if first:
            df_chunk.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
            first = False

        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
    
    print(f"Finished ingesting {table_name}")


def ingest_csv_full(url, table_name, engine, dtype):
    """Handles small CSVs (like zones) by reading fully."""
    df = pd.read_csv(url, dtype=dtype)
    print(f"Read {len(df)} rows. Writing to SQL...")
    
    df.to_sql(name=table_name, con=engine, if_exists='replace')
    print(f"Finished ingesting {table_name}")


def ingest_parquet(url, table_name, engine):
    """Handles Parquet files. Parquet embeds schema, so explicit dtype is rarely needed."""
    # Note: Requires pyarrow or fastparquet installed
    df = pd.read_parquet(url)
    print(f"Read {len(df)} rows from Parquet. Writing to SQL...")
    
    # We use chunksize in to_sql to split the INSERT statements, 
    # avoiding memory issues on the Postgres side for large inserts.
    df.to_sql(name=table_name, con=engine, if_exists='replace', chunksize=100000)
    print(f"Finished ingesting {table_name}")


if __name__ == '__main__':
    run()