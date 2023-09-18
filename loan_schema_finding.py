from pyarrow import dataset as ds, fs, parquet as pq
import pyarrow as pa
from settings import aws_access_key, aws_secret_key, aws_region, fnm_loan_issuance_dest, fre_loan_issuance_dest

loan_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")

s3 = fs.S3FileSystem(access_key=aws_access_key,
                     secret_key=aws_secret_key,
                     region=aws_region)


fnm_loan_data_set = ds.dataset(fnm_loan_issuance_dest, partitioning=loan_part, filesystem=s3)

fragments = fnm_loan_data_set.get_fragments()
fnm_schemas = [fragment.physical_schema for fragment in fragments]


first_schema = fnm_schemas[0]

for schema in fnm_schemas:
    for name in first_schema.names:
        field1 = first_schema.field(name)
        field2 = schema.field(name)
        if field1.type == pa.null() and field2.type != pa.null():
            
            i = schema.get_field_index(name)
            first_schema = schema.set(i, field2)
            

fre_loan_data_set = ds.dataset(fre_loan_issuance_dest, partitioning=loan_part, filesystem=s3)
fragments = fnm_loan_data_set.get_fragments()
fre_schemas = [fragment.physical_schema for fragment in fragments]

for schema in fre_schemas:
    for name in first_schema.names:
        field1 = first_schema.field(name)
        field2 = schema.field(name)
        if field1.type == pa.null() and field2.type != pa.null():
            
            i = schema.get_field_index(name)
            first_schema = schema.set(i, field2)


final_schema = pa.schema(
    [('Loan Identifier', pa.int64()),
    ('Loan Correction Indicator', pa.string()),
    ('Prefix', pa.string()),
    ('Security Identifier', pa.string()),
    ('CUSIP', pa.string()),
    ('Mortgage Loan Amount', pa.float64()),
    ('Issuance Investor Loan UPB', pa.float64()),
    ('Current Investor Loan UPB', pa.float64()),
    ('Amortization Type', pa.string()),
    ('Original Interest Rate', pa.float64()),
    ('Issuance Interest Rate', pa.float64()),
    ('Current Interest Rate', pa.float64()),
    ('Issuance Net Interest Rate', pa.float64()),
    ('Current Net Interest Rate', pa.float64()),
    ('First Payment Date', pa.timestamp('ms')),
    ('Maturity Date', pa.timestamp('ms')),
    ('Loan Term', pa.int64()),
    ('Remaining Months to Maturity', pa.int64()),
    ('Loan Age', pa.int64()),
    ('Loan-To-Value (LTV)', pa.int64()),
    ('Combined Loan-To-Value (CLTV)', pa.int64()),
    ('Debt-To-Income (DTI)', pa.int64()),
    ('Borrower Credit Score', pa.int64()),
    ('Number of Borrowers', pa.int64()),
    ('First Time Home Buyer Indicator', pa.string()),
    ('Loan Purpose', pa.string()),
    ('Occupancy Status', pa.string()),
    ('Number of Units', pa.int64()),
    ('Property Type', pa.string()),
    ('Channel', pa.string()),
    ('Property State', pa.string()),
    ('Seller Name', pa.string()),
    ('Servicer Name', pa.string()),
    ('Mortgage Insurance Percent', pa.int64()),
    ('Mortgage Insurance Cancellation Indicator', pa.string()),
    ('Government Insured Guarantee', pa.string()),
    ('Assumability Indicator', pa.string()),
    ('Interest Only Loan Indicator', pa.string()),
    ('Interest Only First Principal and Interest Payment Date', pa.null()),
    ('Months to Amortization', pa.null()),
    ('Prepayment Penalty Indicator', pa.string()),
    ('Prepayment Penalty Total Term', pa.null()),
    ('Index', pa.int64()),
    ('Mortgage Margin', pa.float64()),
    ('MBS PC Margin', pa.float64()),
    ('Interest Rate Adjustment Frequency', pa.int64()),
    ('Interest Rate Lookback', pa.int64()),
    ('Interest Rate Rounding Method', pa.string()),
    ('Interest Rate Rounding Method Percent', pa.int64()),
    ('Convertibility Indicator', pa.string()),
    ('Initial Fixed Rate Period', pa.int64()),
    ('Next Interest Rate Adjustment Date',pa.timestamp('ms')),
    ('Months to Next Interest Rate Adjustment Date', pa.int64()),
    ('Life Ceiling Interest Rate', pa.float64()),
    ('Life Ceiling Net Interest Rate', pa.float64()),
    ('Life Floor Interest Rate', pa.float64()),
    ('Life Floor Net Interest Rate', pa.float64()),
    ('Initial Interest Rate Cap Up Percent', pa.float64()),
    ('Initial Interest Rate Cap Down Percent', pa.float64()),
    ('Periodic Interest Rate Cap Up Percent', pa.float64()),
    ('Periodic Interest Rate Cap Down Percent', pa.float64()),
    ('Modification Program', pa.string()),
    ('Modification Type', pa.string()),
    ('Number of Modifications', pa.int64()),
    ('Total Capitalized Amount', pa.float64()),
    ('Interest Bearing Mortgage Loan Amount', pa.float64()),
    ('Original Deferred Amount', pa.float64()),
    ('Current Deferred UPB', pa.float64()),
    ('Loan Age As Of Modification', pa.int64()),
    ('Estimated Loan-To-Value (ELTV)', pa.int64()),
    ('Updated Credit Score', pa.int64()),
    ('Interest Rate Step Indicator', pa.string()),
    ('Initial Step Fixed-Rate Period', pa.int64()),
    ('Total Number of Steps', pa.int64()),
    ('Number of Remaining Steps', pa.int64()),
    ('Next Step Rate', pa.null()),
    ('Terminal Step Rate', pa.float64()),
    ('Terminal Step Date', pa.timestamp('ms')),
    ('Step Rate Adjustment Frequency', pa.null()),
    ('Next Step Rate Adjustment Date', pa.null()),
    ('Months to Next Step Rate Adjustment Date', pa.null()),
    ('Periodic Step Cap Up Percent', pa.float64()),
    ('Origination Mortgage Loan Amount', pa.float64()),
    ('Origination Interest Rate', pa.float64()),
    ('Origination Amortization Type', pa.string()),
    ('Origination Interest Only Loan Indicator', pa.string()),
    ('Origination First Payment Date', pa.timestamp('ms')),
    ('Origination Maturity Date', pa.timestamp('ms')),
    ('Origination Loan Term', pa.int64()),
    ('Origination Loan-To-Value (LTV)', pa.int64()),
    ('Origination Combined Loan-To-Value (CLTV)', pa.int64()),
    ('Origination Debt-To-Income Ratio', pa.int64()),
    ('Origination Credit Score', pa.int64()),
    ('Origination Loan Purpose', pa.string()),
    ('Origination Occupancy Status', pa.string()),
    ('Origination Channel', pa.string()),
    ('Days Delinquent', pa.null()),
    ('Loan Performance History', pa.string()),
    ('Loan Participation Percent', pa.float64())]
)
print(final_schema.equals(first_schema))
for i,name in enumerate(first_schema.names):
    field_first = first_schema.field(i)
    field_final = final_schema.field(i)
    if not field_first.equals(field_final):
        print(f"{i}, {field_first.name}, {field_first.type}")
        print(f"{i}, {field_final.name}, {field_final.type}")
    