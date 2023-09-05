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

#query to find average credit score of loans by month
q = """SELECT Issuance_year, Issuance_Month, AVG(l."Borrower Credit Score") AS avg_credit_score 
FROM loan_data_set l
GROUP BY ("Issuance_Year", "Issuance_Month")
ORDER BY avg_credit_score
"""
#query to find count of loans by type
q2 = """SELECT Prefix, COUNT(*) count_by_prefix FROM security_data_set s
GROUP BY Prefix;"""

#query to find SOFR based vs US IBOR based rates for adjustable rate mortgages
q3 = """ SELECT Prefix, Issuance_Year, COUNT(*) FROM security_data_set s
WHERE Prefix IN ('SO', 'LB')
GROUP BY Prefix, Issuance_Year
ORDER BY Issuance_Year ASC;"""


security_types_dict_metadata = {'CL': 'Conventional Long-Term, Level-Payment Mortgages; Single-Family; maturing or due in 30 years or less',
                                'CI': 'Conventional Intermediate-Term, Level-Payment Mortgages; Single-Family; maturing or due in 15 years or less.',
                                'CT': 'Conventional Intermediate-Term, Level-Payment Mortgages;Single-Family; maturing or due in 20 years or less',
                                'SO': 'Conventional Adjustable-Rate Mortgages; Single Family; Secured Overnight Financing Rate (SOFR)',
                                'CK': '30 year flat, but with weird 2008 related loans',
                                'LB': 'Adjustable-Rate Mortgages, Single-Family, Refinitiv USD IBOR Consumer Cash Fallback, lifetime caps are poolspecific.',
                                'CN': 'Conventional Short-Term, Level-Payment Mortgages; Single-Family; maturing or due in 10 years or less.',
                                'I4': 'Conventional Extra Long-Term, Reperforming Modified, Level-Payment Mortgages; Single-Family; maturing or due in 40 years or less.',
                                'RE': 'Conventional Long-Term, Level-Payment Relocation Mortgages; Single-Family.',
                                'CJ': '15 year flat, but with weird 2008 related loans',
                                'R3': 'Conventional Long-Term, Non-Modified Reperforming, Level-Payment Mortgages; Single-Family; maturing or due in 30 years or less.',
                                'CR': """Conventional Long-Term, Level-Payment Mortgages; Single-Family; maturing or due in 30 years or less. The
pool is comprised entirely of mortgages with loan-to-value ratios greater than 125 percent.""",
                                'WS': """Conventional Adjustable-Rate Mortgages; Single-Family. Includes a wide variety of ARM types and
indices.""",
                                'ZL':'Supers security collateralized by REMIC certificates which are directly or indirectly backed by Conventional Long-Term, Level-Payment Mortgages; Single-Family; maturing or due in 30 years or less.'
                                }

#Query to find months with most mortgages issued into mortgage_backed_securities
q4 = """ SELECT Issuance_Year, Issuance_Month, SUM("Issuance Investor Security UPB") AS total_UPB FROM security_data_set s
WHERE Prefix = 'CL'
GROUP BY Issuance_Year, Issuance_Month
ORDER BY total_UPB DESC;"""

#Query to find CR mortgages by year (pools with all LTVs > 125)
q5 = """ SELECT Issuance_Year, SUM("Issuance Investor Security UPB") upb_sum FROM security_data_set s
WHERE Prefix = 'CR'
GROUP BY Issuance_Year
ORDER BY upb_sum DESC;
"""

#Query to find WA debt to income ratios on loans by year (Security level, not perfect) only 30 year flat
q6 =  """SELECT Issuance_Year, AVG("WA Debt-To-Income (DTI)") avg_W_DTI FROM security_data_set s
WHERE prefix = 'CL'
GROUP BY Issuance_Year
ORDER BY avg_W_DTI DESC;"""

#Query to find WA Issuance Interest rate by year only 30 year flat
q7 = """SELECT Issuance_Year, AVG("WA Issuance Interest Rate") avg_issuance_interest, FROM security_data_set s
WHERE prefix = 'CL'
GROUP BY Issuance_Year
ORDER BY avg_issuance_interest ASC"""

#Query to find different loan purposes
q8 = """SELECT "Loan Purpose" loan_purpose, COUNT(*) FROM loan_data_set l
GROUP BY loan_purpose;"""

#query to find cash-out refinance as a percent of loan purpose over time  
q9 = """WITH prelim AS(SELECT Issuance_Year, COUNT(*) c,
SUM(CASE WHEN l."Loan Purpose" = 'C' THEN 1
ELSE 0 END) cash_outs, FROM loan_data_set l
GROUP BY Issuance_Year)

SELECT Issuance_Year, prelim.cash_outs/prelim.c FROM prelim"""

#query for all refinance as a percent of loan purpose over time
q10 = """WITH prelim AS(SELECT Issuance_Year, COUNT(*) c,
SUM(CASE WHEN l."Loan Purpose" = 'C' THEN 1
WHEN l."Loan Purpose" = 'N' THEN 1
ELSE 0 END) cash_outs, FROM loan_data_set l
GROUP BY Issuance_Year)

SELECT Issuance_Year, prelim.cash_outs/prelim.c FROM prelim"""


q11 = """SELECT "First Time Home Buyer Indicator" first_time_indicator, COUNT(*)
FROM loan_data_set
GROUP BY first_time_indicator"""

#query for first time homebuyers as percent of loans over time
q12 = """WITH prelim AS(SELECT Issuance_Year, COUNT(*) c,
SUM(CASE WHEN l."First Time Home Buyer Indicator" = 'Y' THEN 1
ELSE 0 END) first_time, FROM loan_data_set l
GROUP BY Issuance_Year)

SELECT Issuance_Year, prelim.first_time/prelim.c AS pct_first_time FROM prelim
ORDER BY pct_first_time DESC"""

#query for amount of money given in cash out refinances over time
q13 = """SELECT Prefix, Issuance_Year, SUM("Mortgage Loan Amount") sum_loans FROM loan_data_set l
WHERE "Loan Purpose" = 'C'
GROUP BY Issuance_Year, Prefix
ORDER BY sum_loans DESC"""

res = con.execute(q13)
security_types = res.fetchall()
#security_types.sort(key= lambda t: t[1], reverse=True)
print(security_types)