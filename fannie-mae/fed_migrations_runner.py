#creating this just as a basic file to run the fed_holdings migration. do not want to run the whole pipeline for the other files (another reason to use delta-rs)
from migrations.fed_holdings_migration import Fed_Holdings_Migration
from pyarrow import fs
from settings import aws_access_key, aws_secret_key, aws_region, fed_holdings_dest

s3 = fs.S3FileSystem(access_key=aws_access_key,
                     secret_key=aws_secret_key,
                     region=aws_region)



Fed_Holdings_Migration(s3, fed_holdings_dest)()