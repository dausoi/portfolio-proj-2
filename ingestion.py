import datetime as dt
import json
import requests
import pandas as pd
import psycopg2
import shutil
from pathlib import Path
import os

def get_connection(connection_file, schema="public"):
    with open(connection_file, "r") as f:
        conn_config = json.load(f)

    ps_con = psycopg2.connect(host=conn_config["host"],
                            user=conn_config["user"],
                            password=conn_config["password"],
                            database=conn_config["database"],
                            port=conn_config["port"],
                            options=f"-c search_path={schema}"
                                )
    return ps_con

def download_file(year: int, month: int, day: int, hr: int):
    """ Download Raw File from Wikimedia Dump. Located at data/raw. """
    raw_filename = f"pageviews-{year}{month:02d}{day:02d}-{hr:02d}0000"
    URL_PREFIX = f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:02d}"
    url_file = f"{URL_PREFIX}/{raw_filename}.gz"

    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    raw_filepath = os.path.join(raw_dir, raw_filename + ".gz")

    if not os.path.exists(raw_filepath):
        print(f"Downloading file {url_file} ...")

        with requests.get(url_file, stream=True) as r:
            with open(raw_filepath, "wb") as f:
                shutil.copyfileobj(r.raw, f)
    else:
        print(f"Skipping file download {url_file} ...")

    return raw_filepath

def get_dataframe(raw_filepath, nrows=None):
    """ Read Raw file and Return Pandas Dataframe. Ready for Staging Database"""
    filename = raw_filepath.split("/")[-1].split(".")[0]

    print(f"Getting Dataframe.. for {raw_filepath}")
    try:
        wiki_pgview = pd.read_csv(raw_filepath, 
                        delim_whitespace=True, 
                        names=["domain_code", "page_title", "count_views", "total_response_size"],
                        nrows=nrows)
    except pd.errors.ParserError:
        print("Error occured on C engine, switching to Python.")
        wiki_pgview = pd.read_csv(raw_filepath, 
                        delim_whitespace=True, 
                        names=["domain_code", "page_title", "count_views", "total_response_size"],
                        nrows=nrows,
                        engine="python")
    
    wiki_pgview["pgview_datetime"] = str(dt.datetime.strptime(filename, "pageviews-%Y%m%d-%H%M%S"))
    wiki_pgview["insert_time"] = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d %H:%M:%S")
    columns = ["insert_time", "pgview_datetime", "domain_code", "page_title", "count_views", "total_response_size"]
    wiki_pgview = wiki_pgview[columns]

    return wiki_pgview

def write_to_csv(df, csv_filename, replace=False):
    csv_filename = f"{csv_filename}.csv"

    csv_dir = Path("data/csv")
    csv_dir.mkdir(parents=True, exist_ok=True)

    csv_filepath = os.path.join(csv_dir, csv_filename)

    if not os.path.exists(csv_filepath):
        print(f"Writing to CSV {csv_filepath}...")
        df.to_csv(csv_filepath, index=False, chunksize=100000)
    else:
        print("Skipped writing to CSV.")

    return csv_filepath

def create_table(df, table_name, con, replace=False):
    with con as c:
        cur = c.cursor()
        create_table_query = pd.io.sql.get_schema(name=f"{table_name}", con=con, frame=df)
        print(f"Creating empty table named {table_name} ...")
        try:
            cur.execute(create_table_query)
        except psycopg2.errors.DuplicateTable:
            con.rollback()
            print(f"Table already existed.")
            if replace:
                print("Removing old records and replacing with new data...")
                cur.execute(f"DROP TABLE {table_name}")
                cur.execute(create_table_query)
            else:
                print("Ready for data appending.")
        finally:
            con.commit()
        

def copy_from_csv(csv_filepath, table_name, con):
    with con as c:
        cur = c.cursor()
        with open(csv_filepath, "r") as f:
            column_sql = f"({next(f)})"
            print(f"Inserting data from {csv_filepath} into the table {table_name}...")
            cur.copy_expert(f"COPY {table_name} {column_sql} FROM STDIN WITH (FORMAT CSV, DELIMITER ',')", f)
            con.commit()

def download_raw_day(y, m, d):
    raw_paths = []
    for h in range(24):
        rp = download_file(y, m, d, h)
        raw_paths.append(rp)
    return raw_paths

def generate_csv(raw_path, csv_filename, table_name, con, replace=False):
    df = get_dataframe(raw_path)
    create_table(df, table_name, con, replace)
    csv_fp = write_to_csv(df, csv_filename)
    return csv_fp

def generate_csv_day(raw_paths: list[str], table_name, con, replace=False):
    csv_paths = []
    for raw_path in raw_paths:
        csv_filename = raw_path.split("/")[-1].split(".")[0]
        cp = generate_csv(raw_path, csv_filename, table_name, con, replace=replace)
        if replace: replace = False
        csv_paths.append(cp)
    return csv_paths

def copy_from_csv_day(csv_paths, table_name, con):
    for csv_path in csv_paths:
        copy_from_csv(csv_path, table_name, con)


def main(y, m, d, con_file, src_schema):
    table_name = f"pageview_raw_{y}{m:02d}{d:02d}"
    con = get_connection(con_file, src_schema)
    raw_paths = download_raw_day(y, m, d)
    csv_paths = generate_csv_day(raw_paths, table_name, con, replace=True)
    copy_from_csv_day(csv_paths, table_name, con)
    con.close()

if __name__ == "__main__":
    main()
