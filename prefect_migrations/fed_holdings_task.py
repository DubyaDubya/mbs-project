import requests
import pyarrow as pa
from pyarrow import csv, compute as cp
from prefect import task


@task
def migrate_from_fed_api(file_system, destination_path, log_prints=True):
    url = 'https://markets.newyorkfed.org/api/soma/summary.csv'
    resp = requests.get(url)
    print(f"requested from fed API at {url}")
    buf = pa.py_buffer(resp.content)
    with pa.input_stream(buf) as st:
        table = csv.read_csv(st)
    print("csv read into pyarrow")
    date = table.column(0)
    year = cp.year(date)

    month = cp.month(date)
    table = table.add_column(0, "Year", year)
    table = table.add_column(1, "Month", month)
    print("Year and Month added to table")

    with file_system.open_output_stream(destination_path) as out_stream:
        csv.write_csv(table, out_stream)
    print(f"fed data written to s3 at {destination_path}")
    return destination_path
        

