import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs

from settings import loan_issuance_dest, security_issuance_folder, fed_holdings_folder
from settings import aws_access_key, aws_secret_key, aws_region

class Querier:

    def __init__(self, query: str):
        self.query = query
        read_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")
        s3 = fs.S3FileSystem(access_key=aws_access_key,
                     secret_key=aws_secret_key,
                     region=aws_region)

        self.loan_data_set = ds.dataset(loan_issuance_dest, partitioning=read_part, filesystem=s3)
        self.security_data_set = ds.dataset(security_issuance_folder, filesystem=s3)
        self.fed_data_set = ds.dataset(fed_holdings_folder, filesystem=s3, format='csv')
        self.conn = duckdb.connect()

    def __call__(self):
        res = self.conn.execute(self.query)
        return res.fetch_arrow_table()

        






