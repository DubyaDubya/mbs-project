import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs


loan_folder = '***REMOVED***2'
security_folder = '***REMOVED***'

read_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")
s3 = fs.S3FileSystem(access_key='***REMOVED***',
                     secret_key='***REMOVED***',
                     region='***REMOVED***')

loan_data_set = ds.dataset(loan_folder, partitioning=read_part, filesystem=s3)
security_data_set = ds.dataset(security_folder, filesystem=s3)
con = duckdb.connect()

q = """SELECT Issuance_year, Issuance_Month, AVG(l."Borrower Credit Score") AS avg_credit_score 
FROM loan_data_set l
GROUP BY ("Issuance_Year", "Issuance_Month")
ORDER BY avg_credit_score
"""

q2 = """SELECT Issuance_Year, Issuance_Month, SUM("Issuance Investor Security UPB") upb_sum
FROM security_data_set
GROUP BY (Issuance_Year, Issuance_Month)
ORDER BY upb_sum DESC;"""
res = con.execute(q2)
