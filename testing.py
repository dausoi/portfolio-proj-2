import datetime as dt
import os
import requests

def _compare_loc_rm(raw_filepath: str) -> bool:
    """ Check local raw file size with the remote server. Return True if sizes are equal. False otherwise. """
    prefix = "data/raw"
    ts = dt.datetime.strptime(raw_filepath, f"{prefix}/pageviews-%Y%m%d-%H0000.gz")
    url_prefix = f"https://dumps.wikimedia.org/other/pageviews/{ts.year}/{ts.year}-{ts.month:02d}"
    raw_filename = raw_filepath.split("/")[-1]
    url_full = f"{url_prefix}/{raw_filename}"
    try:
        remote_filesize = int(requests.head(url_full).headers["content-length"])
        local_filesize = int(os.path.getsize(raw_filepath))
    except FileNotFoundError:
        local_filesize = -1

    return remote_filesize == local_filesize

def check_file_content(year: int, month: int, day: int) -> None:
    """ Post-download file content check. Notify if there are differences in file sizes between local and server. """
    prefix = "data/raw"
    print("File content/size checking...")
    for hour in range(24):
        raw_filepath = f"{prefix}/pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"
        print(f"Test {hour + 1:02d}/24: {raw_filepath} ...... {'PASS' if _compare_loc_rm(raw_filepath) else 'FAIL'}")

def check_file_completeness(year: int, month: int, day: int) -> None:
    """ Post-download file number check. Notify if there are missing files. """
    # TODO: Implement
    pass
