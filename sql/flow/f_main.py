import datetime as dt
from prefect import flow
import f_ingestion, f_transformation

@flow(log_prints=True)
def main(con_file):
    today = dt.datetime.today() - dt.timedelta(days=2)  # Prefect doesn't support 'dynamic' schedule parameter
    y, m, d = today.year, today.month, today.day    
    raw_table_name = f"pageview_raw_{y}{m:02d}{d:02d}"
    agg_table_name = f"pageview_{y}{m:02d}{d:02d}"
    dest_table_name = f"pageview_{y}"
    f_ingestion.main(y, m, d, con_file, raw_table_name)
    f_transformation.main(raw_table_name, agg_table_name, dest_table_name, con_file)

if __name__ == "__main__":
    main()
