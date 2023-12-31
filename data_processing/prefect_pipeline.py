from prefect import flow
import pyarrow.fs as fs
import pyarrow.dataset as ds
import pyarrow as pa

from prefect_migrations.fed_holdings_task import migrate_from_fed_api
from prefect_migrations.security_issuance_flow import security_issuance_migration
from prefect_migrations.loan_issuance_flow import loan_issuance_migration
from prefect_migrations.duck_db_flow import all_duck_db_queries
from prefect_migrations.combine_securities_task import combine_and_save
from prefect_migrations.combine_loans_task import combine_and_save_loans
from prefect_migrations.final_queries import prod_queries

from settings import fnm_loan_issuance_src, fnm_loan_issuance_dest
from settings import fnm_security_issuance_src, fnm_security_issuance_dest, fnm_security_issuance_folder
from settings import fre_loan_issuance_src, fre_loan_issuance_dest
from settings import fre_security_issuance_src, fre_security_issuance_dest, fre_security_issuance_folder
from settings import fed_holdings_folder, fed_holdings_dest
from settings import full_security_issuance_folder, full_security_issuance_dest, full_loan_issuance_dest
from settings import final_folder
from settings import aws_access_key, aws_secret_key, aws_region

loan_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")

s3 = fs.S3FileSystem(access_key=aws_access_key,
                     secret_key=aws_secret_key,
                     region=aws_region)

@flow(log_prints=True)
def full_pipeline():
    print("starting fed migration")
    migrate_from_fed_api(s3, fed_holdings_dest)
    print(f"fed data written to {fed_holdings_dest}")

    print("starting fannie mae migration")
    security_issuance_migration(s3, fnm_security_issuance_src, fnm_security_issuance_dest)
    print(f"fannie mae security data written to {fnm_security_issuance_dest}")
    loan_issuance_migration(s3, fnm_loan_issuance_src, fnm_loan_issuance_dest, loan_part)
    print(f"fannie mae loan data written to {fnm_loan_issuance_dest}")
    print("finished fannie mae migration")

    print("starting freddie mac migration")
    security_issuance_migration(s3, fre_security_issuance_src, fre_security_issuance_dest)
    print(f"freddie mac security data written to {fre_security_issuance_dest}")
    
    print("starting securities file combination")
    combine_and_save(s3, fnm_security_issuance_dest, fre_security_issuance_dest, full_security_issuance_dest)
    print(f"saved full securities dataset to {full_security_issuance_dest}")

    loan_issuance_migration(s3, fre_loan_issuance_src, fre_loan_issuance_dest, loan_part)
    print(f"freddie mac loan issuance data written to {fre_loan_issuance_dest}")
    print("finished freddie mac migration")

    print("starting loan issuance combination")
    combine_and_save_loans(s3, fnm_loan_issuance_dest, fre_loan_issuance_dest, full_loan_issuance_dest,
                           loan_part)
    print(f"combined loan dataset written to {full_loan_issuance_dest}")
    
    print("starting duckdb")

    fed_data_set = ds.dataset(fed_holdings_folder, filesystem=s3, format='csv')
    security_data_set = ds.dataset(full_security_issuance_dest, filesystem=s3)
    loan_data_set = ds.dataset(full_loan_issuance_dest, filesystem=s3, partitioning=loan_part)

    all_duck_db_queries(s3, loan_data_set, security_data_set, fed_data_set, prod_queries, final_folder)
    print(f"duck db final queries written to {final_folder}")
    print("finished duckdb")
    
    

    print("finished pipeline")





full_pipeline()