import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as fs
from prefect import task, flow

from settings import fnm_loan_issuance_dest, fnm_security_issuance_folder, fed_holdings_folder
from settings import aws_access_key, aws_secret_key, aws_region


read_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")

con = duckdb.connect()

@task
def duck_db_query(filesystem, loan_data_set, security_data_set, fed_data_set, query_number, query, final_dest, log_prints=True):
    file_name = f"query-{query_number}.parquet"
    final_destination = '/'.join((final_dest, file_name,))
    print(f"query-{query_number} started")
    res = con.execute(query)
    query_data = res.fetch_arrow_table()
    print(f"query-{query_number} executed")
    print(query_data)
    pq.write_table(query_data, final_destination, filesystem=filesystem)
    print(f"query-{query_number} written to {final_destination}")

@flow(log_prints=True)
def all_duck_db_queries(filesystem, loan_data_set, security_data_set, fed_data_set, prod_queries, final_dest):
    print(f"started duckdb queries")
    for i, query in enumerate(prod_queries):
        duck_db_query(filesystem, loan_data_set, security_data_set, fed_data_set, i, query, final_dest)
