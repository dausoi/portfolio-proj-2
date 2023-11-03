from prefect import flow, task
import psycopg2

from pathlib import Path
import json


def get_connection(connection_file: str):
    """
    Connect to the PostgreSQL database.

    Parameters:
        - connection_file: json file path to read connection
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

@task
def transformation(src_table: str, agg_table: str, dest_table:str, con):
    """
    Transform data inside PostgreSQL database. Create a table which sums entries with same domain_code.

    Parameters:
        - src_table: Table name of raw pageview Wikimedia data
        - agg_table: Name of aggregated table
        - dest_table: Name of destination table in the production schema
        - con: Connection object
    """        
    src_domain_table = agg_table  # Aggregated table is already group by domain name

    cur = con.cursor()
    print("Creating enum types...")
    cur.execute(Path("sql/pr_create_enums.sql").read_text())
    print("Creating functions....")
    cur.execute(Path("sql/f_extract_domain.sql").read_text())
    print("Creating aggregated tables...")
    cur.execute(Path("sql/create_agg_table.sql").read_text() \
                        .replace("[src_table]", src_table) \
                        .replace("[agg_table]", agg_table))
    print("Applying function with domain code....")
    cur.execute(Path("sql/create_domain_table.sql").read_text() \
                        .replace("[src_domain_table]", src_domain_table))
    print("Creating output view...")
    cur.execute(Path("sql/create_output_table.sql").read_text() \
                        .replace("[agg_table]", agg_table) \
                        .replace("[dest_table]", dest_table))
    print("Cleaning up unused tables...")
    cur.execute(Path("sql/clean_up.sql").read_text() \
                        .replace("[agg_table]", agg_table) \
                        .replace("[src_table]", src_table))
    con.commit()
    cur.close()
        
def main(src_table: str, agg_table: str, dest_table: str, connection_file: str):
    ps_con = get_connection(connection_file)
    transformation(src_table, agg_table, dest_table, ps_con)
    ps_con.close()

if __name__ == "__main__":
    main()
