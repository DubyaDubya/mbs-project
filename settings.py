from dotenv import load_dotenv
import os

load_dotenv()

aws_region = os.environ['AWS_REGION']
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']

fnm_security_issuance_src = os.environ['FNM_SECURITY_ISSUANCE_SRC']
fnm_security_issuance_dest = os.environ['FNM_SECURITY_ISSUANCE_DEST']
fnm_security_issuance_folder = os.environ['FNM_SECURITY_ISSUANCE_FOLDER']

fnm_loan_issuance_src = os.environ['FNM_LOAN_ISSUANCE_SRC']
fnm_loan_issuance_dest = os.environ['FNM_LOAN_ISSUANCE_DEST']

fre_security_issuance_src = os.environ['FRE_SECURITY_ISSUANCE_SRC']
fre_security_issuance_dest = os.environ['FRE_SECURITY_ISSUANCE_DEST']
fre_security_issuance_folder = os.environ['FRE_SECURITY_ISSUANCE_FOLDER']

fre_loan_issuance_src = os.environ['FRE_LOAN_ISSUANCE_SRC']
fre_loan_issuance_dest = os.environ['FRE_LOAN_ISSUANCE_DEST']

full_security_issuance_dest = os.environ['FULL_SECURITY_ISSUANCE_DEST']
full_security_issuance_folder = os.environ['FULL_SECURITY_ISSUANCE_FOLDER']

full_loan_issuance_dest = os.environ['FULL_LOAN_ISSUANCE_DEST']

fed_holdings_dest = os.environ['FED_HOLDINGS_DEST']
fed_holdings_folder = os.environ['FED_HOLDINGS_FOLDER']

final_folder=os.environ['FINAL_QUERIES_FOLDER']