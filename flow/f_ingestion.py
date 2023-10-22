from prefect import flow, task
import datetime
import json
import pandas as pd
import requests

from pathlib import Path
import os
import psycopg2
import shutil

def get_connection(connection_file: str):
    """
    Connect to the PostgreSQL database.

    Parameters:
        - connection_file: json file path to create a connection to PostgreSQL database
        - default_schema: Name of default schema
    """
    with open(connection_file, "r") as f:
        conn_config = json.load(f)

    ps_con = psycopg2.connect(host=conn_config["host"],
                            user=conn_config["user"],
                            password=conn_config["password"],
                            database=conn_config["database"],
                            port=conn_config["port"],
                            options=f"-c search_path=stg"
                                )
    return ps_con

def download_raw_pageviews(year: int, month: int, day: int, hr: int) -> list[str]:
    """
    Download gz dump file from wikimedia server. Stored in "data/raw" directory.
    Returns a list of gz file path string.

    Parameters:
        - year
        - month
        - day
        - hr: Hour of the day (24-hr)
    """
    raw_filename = f"pageviews-{year}{month:02d}{day:02d}-{hr:02d}0000"
    url_prefix = f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:02d}"
    url_full = f"{url_prefix}/{raw_filename}.gz"

    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    raw_path = os.path.join(raw_dir, raw_filename + ".gz")

    if not os.path.exists(raw_path):
        print(f"Downloading file {url_full} ...")

        with requests.get(url_full, stream=True) as r:
            with open(raw_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)
    else:
        print(f"Skipping file download {url_full} ...")

    return raw_path

def get_dataframe(raw_path: str, nrows: int = None):
    """
    Given a wikimedia gz dump file path in string, returns a Pandas Dataframe.

    Parameters:
        - raw_path: gz dump file path from wikimedia
        - nrows: Number of rows to be processed and returned as Dataframe
    """
    filename = raw_path.split("/")[-1].split(".")[0]

    print(f"Getting Dataframe.. for {raw_path}")
    params = {"filepath_or_buffer": raw_path, 
                    "delim_whitespace": True, 
                    "names": ["domain_code", "page_title", "count_views", "total_response_size"],
                    "nrows": nrows}
    try:
        wiki_pgview = pd.read_csv(**params)
    except pd.errors.ParserError:
        print("Error occured on C engine, switching to Python.")
        wiki_pgview = pd.read_csv(**params, engine="python")
        wiki_pgview["domain_code"] = wiki_pgview["domain_code"].replace(to_replace='""', value=float('nan'))
    
    wiki_pgview["pgview_timestamp"] = str(datetime.datetime.strptime(filename, "pageviews-%Y%m%d-%H%M%S"))
    columns = ["pgview_timestamp", "domain_code", "page_title", "count_views", "total_response_size"]
    wiki_pgview = wiki_pgview[columns]

    return wiki_pgview

def write_df_to_csv(raw_path: str, csv_name: str, no_write: bool = False):
    """
    Export Pandas Dataframe to csv files. Exports are created in "data/csv" directory.

    Parameter:
        - df: Pandas Dataframe to be written
        - csv_name: file name of the target csv
        - no_write: [True] to only return combined Path without creating csv, 
        [False] to both write dataframe to csv and return combined Path
    """
    csv_name = f"{csv_name}.csv"

    csv_dir = Path("data/csv")
    csv_dir.mkdir(parents=True, exist_ok=True)

    csv_filepath = os.path.join(csv_dir, csv_name)

    if not no_write:
        df = get_dataframe(raw_path)
        print(f"Writing to CSV {csv_filepath}...")
        df.to_csv(csv_filepath, index=False, chunksize=100000)

    return csv_filepath

def copy_from_csv(csv_path: str, table_name: str, con):
    """
    Import table in csv format to PostgreSQL server.

    Parameter:
        - csv_path: CSV file path to be imported
        - table_name: Name of the table which data is being imported to
        - con: Connection object
    """
    with con as c:
        cur = c.cursor()
        with open(csv_path, "r") as f:
            column_sql = f"({next(f)})"
            print(f"Inserting data from {csv_path} into the table {table_name}...")
            cur.copy_expert(f"COPY {table_name} {column_sql} FROM STDIN WITH (FORMAT CSV, DELIMITER ',')", f)
            con.commit()

def get_csv_path(raw_path: str, csv_name: str, no_write: bool = False):
    if no_write:
        return write_df_to_csv(raw_path, csv_name, no_write)
    csv_fp = write_df_to_csv(raw_path, csv_name)
    return csv_fp

################################################################################################

@task
def create_table(raw_path: str, table_name, con):
    """
    Create a blank new table in a PostgreSQL database.

    Parameter:
        - raw_path: Raw file path to use and create a table
        - table_name: Name of the table to be created in the pgsql server
        - con: Connection object
        - replace: In case of existing table, [True] to remove and create a new one. [False] to skip.
    """
    df = get_dataframe(raw_path, nrows=100)
    with con as c:
        cur = c.cursor()
        create_table_query = pd.io.sql.get_schema(name=f"{table_name}", con=con, frame=df)
        print(f"Creating empty table named {table_name} ...")
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(create_table_query)
        con.commit()

@task
def download_raw_day(y: int, m: int , d: int, hrs: int = None):
    if not hrs: hrs = range(24)
    raw_paths = []
    for h in hrs:
        rp = download_raw_pageviews(y, m, d, h)
        raw_paths.append(rp)
    return raw_paths

@task
def get_csv_path_period(raw_paths: list[str], no_write: bool = False):
    csv_paths = []
    for raw_path in raw_paths:
        csv_filename = raw_path.split("/")[-1].split(".")[0]
        cp = get_csv_path(raw_path, csv_filename, no_write)
        csv_paths.append(cp)
    return csv_paths

@task
def copy_from_csv_day(csv_paths: list[str], table_name: str, con):
    for csv_path in csv_paths:
        copy_from_csv(csv_path, table_name, con)

# @flow(name="wiki-pageview-ingestion", log_prints=True)
def main(year: int, month: int, day: int, connection_file: str, table_name: str, hours = None):
    """
    Download dump files from wikimedia source and convert them to csv, import them to PostgreSQL server.
    Create a raw data table based on the files downloaded.
    
    Raw dump files are saved in data/raw, CSV files in data/csv.

    Parameters:
        - year: Year (4-digit) part of inquiry day
        - month: Month part of inquiry day
        - day: Day part of inquiry day
        - connection_file: json file path to create a connection to PostgreSQL database
        - stg_schema: Schema to create raw table in
        - table_name: Name of Raw Data table
        - hours: Hour part of inquiry day as a iterable. Default is range(24).
    """
    con = get_connection(connection_file)
    raw_paths = download_raw_day(year, month, day, hours)
    create_table(raw_paths[0], table_name, con)
    csv_paths = get_csv_path_period(raw_paths)
    copy_from_csv_day(csv_paths, table_name, con)
    con.close()

if __name__ == "__main__":
    # Prefect deployment
    main()
