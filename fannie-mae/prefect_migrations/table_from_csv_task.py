from prefect import task
from pyarrow import csv

@task
def csv_to_table(file_sytem, csv_path, log_prints=True):
    with file_sytem.open_input_stream(csv_path) as csv_f:
        table = csv.read_csv(csv_f, parse_options=csv.ParseOptions(delimiter='|'))
    print(f"read table from {csv_path}")
    return table