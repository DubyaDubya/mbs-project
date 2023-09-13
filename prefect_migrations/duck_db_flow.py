import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as fs
from prefect import task, flow

from settings import loan_issuance_dest, security_issuance_folder, fed_holdings_folder
from settings import aws_access_key, aws_secret_key, aws_region


read_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")
s3 = fs.S3FileSystem(access_key=aws_access_key,
                     secret_key=aws_secret_key,
                     region=aws_region)

loan_data_set = ds.dataset(loan_issuance_dest, partitioning=read_part, filesystem=s3)
security_data_set = ds.dataset(security_issuance_folder, filesystem=s3)
fed_data_set = ds.dataset(fed_holdings_folder, filesystem=s3, format='csv')

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
    pq.write_table(query_data, final_destination, filesystem=s3)
    print(f"query-{query_number} written to {final_destination}")

@flow(log_prints=True)
def all_duck_db_queries(filesystem, loan_data_set, security_data_set, fed_data_set, prod_queries, final_dest):
    print(f"started duckdb queries")
    for i, query in enumerate(prod_queries):
        duck_db_query(filesystem, loan_data_set, security_data_set, fed_data_set, i, query, final_dest)
