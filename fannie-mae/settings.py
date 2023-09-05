from dotenv import load_dotenv
import os

load_dotenv()

aws_region = os.environ['AWS_REGION']
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']

security_issuance_src = os.environ['SECURITY_ISSUANCE_SRC']
security_issuance_dest = os.environ['SECURITY_ISSUANCE_DEST']

loan_issuance_src = os.environ['LOAN_ISSUANCE_SRC']
loan_issuance_dest = os.environ['LOAN_ISSUANCE_DEST']