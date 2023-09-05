from migrations.security_issuance_migration import Security_Issuance_Files_Migration
from migrations.loan_issuance_migration import Loan_Issuance_Files_Migration
from pyarrow import fs 
from settings import aws_region, aws_access_key, aws_secret_key
from settings import security_issuance_src, security_issuance_dest
from settings import loan_issuance_src, loan_issuance_dest

if __name__ == "__main__":

    #Assumes appropriate src files have already been downloaded, unzipped, gziped and uploaded to bucket
    #Assumption is necessary because files need to be manually downloaded from fannie mae pooltalk
    #ISSUE: DOES NOT DO DELTA, INSTEAD DOES FULL MIGRATION EACH TIME. NBD, but could be mitigated with delta-rs

    #Get access to s3 bucket
    s3 = fs.S3FileSystem(access_key=aws_access_key,
                     secret_key=aws_secret_key,
                     region=aws_region)
    #create migration with appropriate s3 bucket, src files and destination
    security_mig = Security_Issuance_Files_Migration(s3, security_issuance_src, security_issuance_dest)
    security_mig()

    loan_mig = Loan_Issuance_Files_Migration(s3, loan_issuance_src, loan_issuance_dest)
    loan_mig()

    #Script works!