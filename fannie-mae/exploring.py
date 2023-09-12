import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs

from settings import loan_issuance_dest, security_issuance_folder, fed_holdings_folder
from settings import aws_access_key, aws_secret_key, aws_region

import plotly.graph_objects as go
from plotly.subplots import make_subplots


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

#query for fed holdings
q14 = """SELECT * FROM fed_data_set f LIMIT 50;"""

#query for avg holdings in a month
q15 = """SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
GROUP BY Year, Month;"""


#query for earliest day of issuance
q16 = """SELECT MIN("Issue Date") FROM security_data_set s"""
first_issuance_date = "2019-06-01"

#query for fed on or after said date 
q17 = """SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= ?
GROUP BY Year, Month;"""
# res = con.execute(q17,[first_issuance_date])

#query to find NOT WEIGHTED average issuance interest rate of conventional flat_rate_mortgages of each type (CL, CI, CT, CN) for each month
q18 = """SELECT Issuance_Year, Issuance_Month, AVG(CASE 
WHEN Prefix = 'CL' THEN "WA Issuance Interest Rate"
ELSE NULL END) AS _30_avg_not_weighted, SUM(CASE
WHEN Prefix = 'CL' THEN "Issuance Investor Security UPB" ELSE 0 END) AS _30_year_UPB,
AVG(CASE 
WHEN Prefix = 'CI' THEN "WA Issuance Interest Rate"
ELSE NULL END) AS _15_avg_not_weighted, SUM(CASE
WHEN Prefix = 'CI' THEN "Issuance Investor Security UPB" ELSE 0 END) AS _15_year_UPB,
AVG(CASE 
WHEN Prefix = 'CT' THEN "WA Issuance Interest Rate"
ELSE NULL END) AS _20_avg_not_weighted, SUM(CASE
WHEN Prefix = 'CT' THEN "Issuance Investor Security UPB" ELSE 0 END) AS _20_year_UPB,
AVG(CASE 
WHEN Prefix = 'CN' THEN "WA Issuance Interest Rate"
ELSE NULL END) AS _10_avg_not_weighted, SUM(CASE
WHEN Prefix = 'CN' THEN "Issuance Investor Security UPB" ELSE 0 END) AS _10_year_UPB
FROM security_data_set s
GROUP BY Issuance_Year, Issuance_Month
"""

# average loan interest rate by purchase purpose
q19 = """SELECT "Loan Purpose" purpose, AVG("Issuance Interest Rate") FROM loan_data_set l
WHERE Prefix IN ('CL','CT', 'CN', 'CI')
GROUP BY purpose"""

#loan type breakdown by purpose
q20 = """WITH counts AS (SELECT "Loan Purpose" purpose, COUNT(*) as cnt, COUNT(CASE WHEN Prefix = 'CL' THEN 1 ELSE NULL END) as cl_cnt, cl_cnt/cnt AS cl_pct,
COUNT(CASE WHEN Prefix = 'CI' THEN 1 ELSE NULL END) as ci_cnt, ci_cnt/cnt AS ci_pct,
COUNT(CASE WHEN Prefix = 'CT' THEN 1 ELSE NULL END) as ct_cnt, ct_cnt/cnt AS ct_pct,
COUNT(CASE WHEN Prefix = 'SO' THEN 1 ELSE NULL END) as so_cnt, so_cnt/cnt AS so_pct,
COUNT(CASE WHEN Prefix = 'CK' THEN 1 ELSE NULL END) as ck_cnt, ck_cnt/cnt AS ck_pct,
COUNT(CASE WHEN Prefix = 'LB' THEN 1 ELSE NULL END) as lb_cnt, lb_cnt/cnt AS lb_pct,
COUNT(CASE WHEN Prefix = 'CN' THEN 1 ELSE NULL END) as cn_cnt, cn_cnt/cnt AS cn_pct,
COUNT(CASE WHEN Prefix = 'CR' THEN 1 ELSE NULL END) as cr_cnt, cr_cnt/cnt AS cr_pct
FROM loan_data_set l GROUP BY purpose)

SELECT purpose, cl_pct, ci_pct, ct_pct, so_pct, ck_pct, lb_pct, cn_pct, cr_pct FROM counts;
"""

#mortgage insurance 

q21 = """SELECT "Mortgage Insurance Percent", "Loan-To-Value (LTV)", "Combined Loan-To-Value (CLTV)" FROM loan_data_set l LIMIT 5"""

#average mortgage insurance  by year
q22 = """SELECT Issuance_Year, AVG("Mortgage Insurance Percent") mi 
FROM loan_data_set l
WHERE "Loan Purpose" = 'P'
GROUP BY Issuance_Year"""

#Weighted average mortgage rates of CL mortgages by month and year, from securities dataset
q23 = """SELECT Issuance_Year, Issuance_Month, SUM("Issuance Investor Security UPB" * "WA Issuance Interest Rate")/ SUM("Issuance Investor Security UPB") AS weighted_avg_int
FROM security_data_set s
WHERE Prefix = 'CL'
GROUP BY Issuance_Year, Issuance_Month
ORDER BY Issuance_Year, Issuance_Month ASC
"""
#Weighted average mortgage rates of CL mortgages by issuance (not origination or payment start date)month and year, from loan dataset
q24 = """SELECT Issuance_Year, Issuance_Month, SUM("Issuance Investor Loan UPB" * "Issuance Interest Rate")/ SUM("Issuance Investor Loan UPB") AS weighted_avg_int
FROM loan_data_set l
WHERE Prefix = 'CL'
GROUP BY Issuance_Year, Issuance_Month
ORDER BY Issuance_Year, Issuance_Month ASC
"""
#INCONSISTENT! need to figure out which to trust/ what the issue is
#First make sure that UPBs and 
upb_from_loans = """SELECT "Security Identifier" security_id, SUM("Mortgage Loan Amount")sum_m_upb, SUM("Issuance Investor Loan UPB") sum_l_upb
FROM loan_data_set l
GROUP BY security_id"""

upb_from_securities = """SELECT "Security Identifier" security_id, "Issuance Investor Security UPB" security_upb
FROM security_data_set s"""

all_upb_methods = """WITH loan_source AS (SELECT "Security Identifier" security_id, SUM("Mortgage Loan Amount")sum_m_upb, SUM("Issuance Investor Loan UPB") sum_l_upb
FROM loan_data_set l
GROUP BY security_id),
security_source AS (SELECT "Security Identifier" security_id, "Issuance Investor Security UPB" security_upb
FROM security_data_set s)
SELECT * FROM loan_source JOIN security_source ON loan_source.security_id = security_source.security_id;"""

upb_avg_dev = """WITH loan_source AS (SELECT "Security Identifier" security_id, SUM("Mortgage Loan Amount")sum_m_upb, SUM("Issuance Investor Loan UPB") sum_l_upb
FROM loan_data_set l
GROUP BY security_id),
security_source AS (SELECT "Security Identifier" security_id, "Issuance Investor Security UPB" security_upb
FROM security_data_set s),
devs AS
(SELECT loan_source.security_id, ABS((sum_m_upb - security_upb)/security_upb) AS m_frac_diff, ABS((sum_l_upb - security_upb)/security_upb) AS 
l_frac_diff, security_upb FROM loan_source JOIN security_source ON loan_source.security_id = security_source.security_id)
SELECT AVG(m_frac_diff) avg_m_diff, AVG(l_frac_diff) avg_l_diff FROM devs"""
#sum of m_UPBs within .3% of security_upb, sum of l_upb within .04% of security_upb. doesn't seem to be the problem

#retry with only CLs
upb_avg_dev_cl_only = """WITH loan_source AS (SELECT "Security Identifier" security_id, SUM("Mortgage Loan Amount")sum_m_upb, SUM("Issuance Investor Loan UPB") sum_l_upb
FROM loan_data_set l
WHERE Prefix = 'CL'
GROUP BY security_id),
security_source AS (SELECT "Security Identifier" security_id, "Issuance Investor Security UPB" security_upb
FROM security_data_set s),
devs AS
(SELECT loan_source.security_id, ABS((sum_m_upb - security_upb)/security_upb) AS m_frac_diff, ABS((sum_l_upb - security_upb)/security_upb) AS 
l_frac_diff, security_upb FROM loan_source JOIN security_source ON loan_source.security_id = security_source.security_id)
SELECT AVG(m_frac_diff) avg_m_diff, AVG(l_frac_diff) avg_l_diff FROM devs"""
# .2% and .04%, UPB sums are not a meaningful source of the issue

wa_int_dev = """WITH loan_source AS (SELECT "Security Identifier" security_id, SUM("Issuance Investor Loan UPB" * "Issuance Interest Rate")/ SUM("Issuance Investor Loan UPB") AS w_avg_i,
SUM("Issuance Investor Loan UPB" * "Original Interest Rate")/ SUM("Issuance Investor Loan UPB") AS w_avg_o
FROM loan_data_set l
WHERE Prefix = 'CL'
GROUP BY security_id),
security_source AS (SELECT "Security Identifier" security_id, "WA Issuance Interest Rate" security_w_avg_int
FROM security_data_set s),
joined AS (SELECT loan_source.security_id, w_avg_i, w_avg_o, security_w_avg_int FROM loan_source 
JOIN security_source ON loan_source.security_id = security_source.security_id),
devs AS (SELECT ABS((w_avg_i - security_w_avg_int)/ security_w_avg_int) AS w_avg_i_dev,
ABS((w_avg_o - security_w_avg_int) / security_w_avg_int) AS w_avg_o_dev FROM joined)

SELECT AVG(w_avg_i_dev), AVG(w_avg_o_dev) FROM devs"""
#both are basically the same, off by 0.0057%. must have been a different issue (probably related to security_ids)

#query counting security_id repeats across issuance_months
id_repeats = """WITH month_count AS (SELECT "Security Identifier" security_id, COUNT(DISTINCT Issuance_Month) cnt  FROM loan_data_set l
GROUP BY security_id)
SELECT COUNT(security_id) bad_ones FROM month_count
WHERE cnt != 1""" # no security ids in multiiple issuance months

#counts of loans not in securities and securiies not in loans
wa_int_loans_and_securities = """WITH loan_source AS (SELECT "Security Identifier" security_id, MAX(Issuance_Year) AS Issuance_Year, MAX(Issuance_Month) AS Issuance_Month,
SUM("Issuance Investor Loan UPB" * "Issuance Interest Rate")/ SUM("Issuance Investor Loan UPB") AS w_avg_int, SUM("Issuance Investor Loan UPB") AS upb
FROM loan_data_set l
WHERE Prefix = 'CL'
GROUP BY security_id),
 
security_source AS (SELECT "Security Identifier" security_id, SUM("Issuance Investor Security UPB" * "WA Issuance Interest Rate")/ SUM("Issuance Investor Security UPB") AS weighted_avg_int
FROM security_data_set s
WHERE Prefix = 'CL'
GROUP BY security_id)

SELECT 'in_loan_not_security' as which_file, COUNT(*) as cnt FROM loan_source LEFT JOIN security_source 
ON loan_source.security_id = security_source.security_id
WHERE security_source.security_id IS NULL
UNION ALL
SELECT 'in_security_not_loan' as which_file, COUNT(*) as cnt FROM security_source LEFT JOIN loan_source
ON security_source.security_id = loan_source.security_id
WHERE loan_source.security_id IS NULL
"""
#10648 securities with no loans assigned

#upb in issuance table, not loans total
upb_iss_minus_loans = """SELECT 'securities' AS source, SUM("Issuance Investor Security UPB") as sum
FROM security_data_set s
UNION ALL
SELECT 'loans' AS source, SUM("Issuance Investor Loan UPB") as sum FROM loan_data_set l"""
#5.07 vs 4.00

#same, only CL
upb_iss_minus_loans_cl = """SELECT 'securities' AS source, SUM("Issuance Investor Security UPB") as sum
FROM security_data_set s
WHERE Prefix = 'CL'
UNION ALL
SELECT 'loans' AS source, SUM("Issuance Investor Loan UPB") as sum FROM loan_data_set l
WHERE Prefix = 'CL'"""
#3.97 in securities, 3.14 in loans

#finding securities in security file and not in loan file
q_questionable_securities = """WITH loan_securities AS (SELECT "Security Identifier" security_id FROM loan_data_set l
GROUP BY security_id)

SELECT s."Security Identifier" AS security_id FROM security_data_set s
LEFT JOIN loan_securities l ON l.security_id = s."Security Identifier"
WHERE l.security_id IS NULL
"""
#These seem to be securities/megas, or collapsed. AKA they're all resecuritizations of old loans or pools that never happened.
#Not what we're looking for, so the loan data set is better.

#One issue here is that these loans are by security_issuance month, not by origination or first payment date
#origination date is not available, and would probably be most appropriate. payment date might also be appropriate
#we'll just use issuance date anyways

# I'll choose to the lons dataset as a source of truth

wa_int_loans = """WITH loan_ints AS (SELECT "Security Identifier" security_id, ANY_VALUE(Issuance_Year) AS Issuance_Year, ANY_VALUE(Issuance_Month) AS Issuance_Month,
SUM("Issuance Investor Loan UPB" * "Issuance Interest Rate")/ SUM("Issuance Investor Loan UPB") AS w_avg_int, SUM("Issuance Investor Loan UPB") AS upb
FROM loan_data_set l
WHERE Prefix = 'CL'
GROUP BY security_id)

SELECT Issuance_Year, Issuance_Month, SUM(w_avg_int*upb) / SUM(upb) AS w_avg_int_monthly
FROM loan_ints
GROUP BY Issuance_Year, Issuance_Month
"""

prod_queries = []

#this is the query for the first table to be used in streamlit. query is interest rate by month, plotted also with fed_holdings
loan_ints_vs_fed_holdings = """WITH loan_ints AS (SELECT "Security Identifier" security_id, ANY_VALUE(Issuance_Year) AS Issuance_Year, ANY_VALUE(Issuance_Month) AS Issuance_Month,
SUM("Issuance Investor Loan UPB" * "Issuance Interest Rate")/ SUM("Issuance Investor Loan UPB") AS w_avg_int, SUM("Issuance Investor Loan UPB") AS upb
FROM loan_data_set l
WHERE Prefix = 'CL'
GROUP BY security_id),

w_avg_ints AS (SELECT Issuance_Year, Issuance_Month, SUM(w_avg_int*upb) / SUM(upb) AS w_avg_int
FROM loan_ints
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month)

SELECT Year, Month, w_avg_int, avg_total_holdings, avg_mbs_holdings FROM w_avg_ints
JOIN fed_holdings ON w_avg_ints.Issuance_Year = fed_holdings.Year 
AND w_avg_ints.Issuance_Month = fed_holdings.Month
ORDER BY Year, Month ASC;
"""
loan_ints_vs_fed_purchases = """WITH loan_ints AS (SELECT "Security Identifier" security_id, ANY_VALUE(Issuance_Year) AS Issuance_Year, ANY_VALUE(Issuance_Month) AS Issuance_Month,
SUM("Issuance Investor Loan UPB" * "Issuance Interest Rate")/ SUM("Issuance Investor Loan UPB") AS w_avg_int, SUM("Issuance Investor Loan UPB") AS upb
FROM loan_data_set l
WHERE Prefix = 'CL'
GROUP BY security_id),

w_avg_ints AS (SELECT Issuance_Year, Issuance_Month, SUM(w_avg_int*upb) / SUM(upb) AS w_avg_int
FROM loan_ints
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month),

fed_purchases AS (SELECT Year, Month, total_holdings - LAG(total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
mbs_holdings - LAG(mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, w_avg_int, total_purchases, mbs_purchases FROM w_avg_ints
JOIN fed_purchases ON w_avg_ints.Issuance_Year = fed_purchases.Year 
AND w_avg_ints.Issuance_Month = fed_purchases.Month
ORDER BY Year, Month ASC;
"""
prod_query_1= loan_ints_vs_fed_purchases
prod_queries.append(prod_query_1)
# works!

fed_holdings_month_before = """WITH fed_holdings AS (SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-05-01'
GROUP BY Year, Month)

SELECT * FROM fed_holdings
"""
#2019-05 avg_total_holdings 3.66635423794372e+12 avg_mbs_holdings 1.568056830344e+12

#Issuance_UPB vs fed holdings
upb_and_fed_holdings = """WITH fed_holdings AS (SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month),

loan_issuance AS (SELECT "Security Identifier" security_id, ANY_VALUE(Issuance_Year) AS Issuance_Year, ANY_VALUE(Issuance_Month) AS Issuance_Month,
SUM("Issuance Investor Loan UPB") AS issuance_upb
FROM loan_data_set l
GROUP BY security_id),

issuance_by_time AS (SELECT Issuance_Year, Issuance_Month, SUM(issuance_upb) as total_issuance_upb FROM loan_issuance
GROUP BY Issuance_Year, Issuance_Month)

SELECT Year, Month, avg_total_holdings, avg_mbs_holdings, total_issuance_upb FROM fed_holdings
JOIN issuance_by_time ON fed_holdings.Year = issuance_by_time.Issuance_Year AND fed_holdings.Month = issuance_by_time.Issuance_Month
ORDER BY Year, Month ASC
"""

upb_and_fed_purchases = """WITH fed_holdings AS (SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, avg_total_holdings - LAG(avg_total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
avg_mbs_holdings - LAG(avg_mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings),

loan_issuance AS (SELECT "Security Identifier" security_id, ANY_VALUE(Issuance_Year) AS Issuance_Year, ANY_VALUE(Issuance_Month) AS Issuance_Month,
SUM("Issuance Investor Loan UPB") AS issuance_upb
FROM loan_data_set l
GROUP BY security_id),

issuance_by_time AS (SELECT Issuance_Year, Issuance_Month, SUM(issuance_upb) as total_issuance_upb FROM loan_issuance
GROUP BY Issuance_Year, Issuance_Month)

SELECT Year, Month, total_purchases, mbs_purchases, total_issuance_upb FROM fed_purchases
JOIN issuance_by_time ON fed_purchases.Year = issuance_by_time.Issuance_Year AND fed_purchases.Month = issuance_by_time.Issuance_Month
ORDER BY Year, Month ASC
"""

prod_query_2 = upb_and_fed_purchases
prod_queries.append(prod_query_2)

#query for upb by channel on the same axis as fed purchases
upb_by_channel_vs_holdings = """WITH totals AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN Channel = 'R' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS retail_upb,
SUM(CASE WHEN Channel = 'C' THEN "Issuance Investor Loan UPB" ELSE 0 END ) AS correspondent_upb,
SUM(CASE WHEN Channel = 'B' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS broker_upb,
SUM(CASE WHEN Channel = '' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS unkown_upb,
SUM("Issuance Investor Loan UPB") AS total_upb
FROM loan_data_set
GROUP BY Issuance_Year, Issuance_Month
ORDER BY Issuance_Year, Issuance_Month ASC),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC)

SELECT Year, Month, total_holdings, mbs_holdings, retail_upb, correspondent_upb,
broker_upb, unkown_upb, total_upb FROM fed_holdings
JOIN totals ON fed_holdings.Year = totals.Issuance_Year AND fed_holdings.Month = totals.Issuance_Month
ORDER BY Year, Month ASC"""

#query for upb by channel on the same axis as fed purchases
upb_by_channel_vs_purchases = """WITH totals AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN Channel = 'R' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS retail_upb,
SUM(CASE WHEN Channel = 'C' THEN "Issuance Investor Loan UPB" ELSE 0 END ) AS correspondent_upb,
SUM(CASE WHEN Channel = 'B' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS broker_upb,
SUM(CASE WHEN Channel = '' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS unkown_upb,
SUM("Issuance Investor Loan UPB") AS total_upb
FROM loan_data_set
GROUP BY Issuance_Year, Issuance_Month
ORDER BY Issuance_Year, Issuance_Month ASC),

fed_holdings AS (SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, avg_total_holdings - LAG(avg_total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
avg_mbs_holdings - LAG(avg_mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, total_purchases, mbs_purchases, retail_upb, correspondent_upb,
broker_upb, unkown_upb, total_upb FROM fed_purchases
JOIN totals ON fed_purchases.Year = totals.Issuance_Year AND fed_purchases.Month = totals.Issuance_Month
ORDER BY Year, Month ASC"""


prod_query_3 = upb_by_channel_vs_purchases
prod_queries.append(prod_query_3)

#query loan issuance upb broken down by loan purpose vs fed_holdings
purpose_upb_vs_fed_holdings = """WITH upb_by_purpose AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "Loan Purpose" = 'C' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS cash_out_refi,
SUM(CASE WHEN "Loan Purpose" = 'N' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS no_cash_refi,
SUM(CASE WHEN "Loan Purpose" = 'P' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS purchase,
SUM(CASE WHEN "Loan Purpose" = 'M' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS modified_loss_mitigation,
SUM("Issuance Investor Loan UPB") AS total
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC)

SELECT Year, Month, cash_out_refi, no_cash_refi, purchase, modified_loss_mitigation, total_holdings, mbs_holdings
FROM fed_holdings JOIN upb_by_purpose ON fed_holdings.Year = upb_by_purpose.Issuance_Year AND fed_holdings.Month = upb_by_purpose.Issuance_Month"""

#query loan issuance upb broken down by loan purpose vs fed_purchases
purpose_upb_vs_fed_purchases = """WITH upb_by_purpose AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "Loan Purpose" = 'C' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS cash_out_refi,
SUM(CASE WHEN "Loan Purpose" = 'N' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS no_cash_refi,
SUM(CASE WHEN "Loan Purpose" = 'P' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS purchase,
SUM(CASE WHEN "Loan Purpose" = 'M' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS modified_loss_mitigation,
SUM("Issuance Investor Loan UPB") AS total
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, avg_total_holdings - LAG(avg_total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
avg_mbs_holdings - LAG(avg_mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, cash_out_refi, no_cash_refi, purchase, modified_loss_mitigation, total_purchases, mbs_purchases
FROM upb_by_purpose JOIN fed_purchases ON upb_by_purpose.Issuance_Year = fed_purchases.Year AND upb_by_purpose.Issuance_Month = fed_purchases.Month"""
prod_query_4 = purpose_upb_vs_fed_purchases
prod_queries.append(prod_query_4)

#query 5 is same, but just cashout refinances
cashout_vs_fed_holdings = """WITH upb_by_purpose AS (SELECT Issuance_Year, Issuance_Month,
SUM("Issuance Investor Loan UPB") AS cash_out_refi
FROM loan_data_set l
WHERE "Loan Purpose" = 'C'
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC)

SELECT Year, Month, cash_out_refi, total_holdings, mbs_holdings
FROM fed_holdings JOIN upb_by_purpose ON fed_holdings.Year = upb_by_purpose.Issuance_Year AND fed_holdings.Month = upb_by_purpose.Issuance_Month"""

#query loan issuance upb broken down by loan purpose vs fed_purchases
cashout_vs_fed_purchases = """WITH upb_by_purpose AS (SELECT Issuance_Year, Issuance_Month,
SUM("Issuance Investor Loan UPB") AS cash_out_refi
FROM loan_data_set l
WHERE "Loan Purpose" = 'C'
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) avg_total_holdings, AVG(MBS) avg_mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, avg_total_holdings - LAG(avg_total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
avg_mbs_holdings - LAG(avg_mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, cash_out_refi, total_purchases, mbs_purchases
FROM upb_by_purpose JOIN fed_purchases ON upb_by_purpose.Issuance_Year = fed_purchases.Year AND upb_by_purpose.Issuance_Month = fed_purchases.Month"""
prod_query_5 = cashout_vs_fed_purchases
prod_queries.append(prod_query_5)

#now first_time home_buyers
#upb to first time homebuyers and non-first timers with fed holdings
first_time_upb_vs_fed_holdings = """WITH upb_by_first_time AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "First Time Home Buyer Indicator" = 'Y' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS first_time_upb,
SUM(CASE WHEN "First Time Home Buyer Indicator" = 'N' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS not_first_time_upb
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC)

SELECT Year, Month, first_time_upb, not_first_time_upb, total_holdings, mbs_holdings
FROM fed_holdings JOIN upb_by_first_time ON fed_holdings.Year = upb_by_first_time.Issuance_Year 
AND fed_holdings.Month = upb_by_first_time.Issuance_Month"""

#upb to first time homebuyers and non-first timers with fed holdings
first_time_upb_vs_fed_purchases = """WITH upb_by_first_time AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "First Time Home Buyer Indicator" = 'Y' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS first_time_upb,
SUM(CASE WHEN "First Time Home Buyer Indicator" = 'N' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS not_first_time_upb,
first_time_upb + not_first_time_upb AS total
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

upb_ft_frac AS (SELECT Issuance_Year, Issuance_Month, first_time_upb / total AS first_time_upb_frac, 
not_first_time_upb / total AS not_first_time_frac FROM upb_by_first_time),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, total_holdings - LAG(total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
mbs_holdings - LAG(mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, first_time_upb_frac, not_first_time_frac, total_purchases, mbs_purchases
FROM fed_purchases JOIN upb_ft_frac ON fed_purchases.Year = upb_ft_frac.Issuance_Year 
AND fed_purchases.Month = upb_ft_frac.Issuance_Month"""
prod_query_6 = first_time_upb_vs_fed_purchases
prod_queries.append(prod_query_6)

#now occupancy status
occupancy = """SELECT "Occupancy Status" occupancy, COUNT(*) FROM loan_data_set l GROUP BY occupancy"""
occupancy_upb_vs_fed_holdings = """WITH upb_by_occupancy AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "Occupancy Status" = 'P' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS primary_residence_upb,
SUM(CASE WHEN "Occupancy Status" = 'S' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS secondary_residence_upb,
SUM(CASE WHEN "Occupancy Status" = 'I' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS investment_property_upb,
SUM(CASE WHEN "Occupancy Status" = '' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS unknown_upb
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC)

SELECT Year, Month, primary_residence_upb, secondary_residence_upb, investment_property_upb, unknown_upb,
total_holdings, mbs_holdings
FROM fed_holdings JOIN upb_by_occupancy ON fed_holdings.Year = upb_by_occupancy.Issuance_Year 
AND fed_holdings.Month = upb_by_occupancy.Issuance_Month"""

occupancy_upb_vs_fed_purchases = """WITH upb_by_occupancy AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "Occupancy Status" = 'P' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS primary_residence_upb,
SUM(CASE WHEN "Occupancy Status" = 'S' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS secondary_residence_upb,
SUM(CASE WHEN "Occupancy Status" = 'I' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS investment_property_upb,
SUM(CASE WHEN "Occupancy Status" = '' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS unknown_upb,
primary_residence_upb +  secondary_residence_upb + investment_property_upb + unknown_upb AS total
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

upb_occupancy_frac AS (SELECT Issuance_Year, Issuance_Month, primary_residence_upb/ total AS primary_residence_fraction,
secondary_residence_upb/ total AS secondary_residence_fraction, investment_property_upb/ total AS investment_property_fraction,
unknown_upb / total AS unknown_fraction FROM upb_by_occupancy),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, total_holdings - LAG(total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
mbs_holdings - LAG(mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, primary_residence_fraction, secondary_residence_fraction, investment_property_fraction, unknown_fraction,
total_purchases, mbs_purchases
FROM fed_purchases JOIN upb_occupancy_frac ON fed_purchases.Year = upb_occupancy_frac.Issuance_Year 
AND fed_purchases.Month = upb_occupancy_frac.Issuance_Month"""
#no large change in purchase purpose


prod_query_7 = first_time_upb_vs_fed_purchases
prod_queries.append(prod_query_7)

#number of units
number_of_units_vs_fed_holdings = """WITH upb_by_units AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "Number of Units" = 1 THEN "Issuance Investor Loan UPB" ELSE 0 END) AS single_unit,
SUM(CASE WHEN "Number of Units" > 1 THEN "Issuance Investor Loan UPB" ELSE 0 END) AS multi_unit,
single_unit + multi_unit AS total
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC)

SELECT Year, Month, single_unit, multi_unit, total, 
total_holdings, mbs_holdings
FROM fed_holdings JOIN upb_by_units ON fed_holdings.Year = upb_by_units.Issuance_Year 
AND fed_holdings.Month = upb_by_units.Issuance_Month"""

number_of_units_vs_fed_purchases = """WITH upb_by_units AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "Number of Units" = 1 THEN "Issuance Investor Loan UPB" ELSE 0 END) AS single_unit,
SUM(CASE WHEN "Number of Units" > 1 THEN "Issuance Investor Loan UPB" ELSE 0 END) AS multi_unit,
single_unit + multi_unit AS total
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

upb_units_frac AS (SELECT Issuance_Year, Issuance_Month, single_unit/ total AS single_unit_fraction,
multi_unit/ total AS multi_unit_fraction FROM upb_by_units),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, total_holdings - LAG(total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
mbs_holdings - LAG(mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, single_unit_fraction, multi_unit_fraction,
total_purchases, mbs_purchases
FROM fed_purchases JOIN upb_units_frac ON fed_purchases.Year = upb_units_frac.Issuance_Year 
AND fed_purchases.Month = upb_units_frac.Issuance_Month"""
#no significant change in number of multi_units

prod_query_8 = number_of_units_vs_fed_purchases
prod_queries.append(prod_query_8)

res = con.execute(prod_query_1)
fetched_results = res.fetchall()

y_m_array = ['-'.join((str(result[0]), str(result[1]),)) for result in fetched_results]
w_avg_int = [result[2] for result in fetched_results]
total_purchases = [result[3] for result in fetched_results]
mbs_purchases = [result[4] for result in fetched_results]

fig1 = make_subplots(specs=[[{"secondary_y": True}]])
fig1.add_trace(
    go.Scatter(x=y_m_array, y=w_avg_int, name="first_time_buyer"),
    secondary_y=False,
)
fig1.add_trace(
    go.Scatter(x=y_m_array, y=total_purchases, name="fed total purchases"),
    secondary_y=True,
)
fig1.add_trace(
    go.Scatter(x=y_m_array, y=mbs_purchases, name="fed mbs purchases"),
    secondary_y=True,
)
fig1.update_layout(
    title_text="weighted average interest rates"
)
fig1.update_xaxes(title_text="year and month")
fig1.update_yaxes(title_text="<b>primary</b> buyers by prior ownership %", secondary_y=False)
fig1.update_yaxes(title_text="<b>secondary</b> fed purchases %", secondary_y=True)
fig1.show()


"""
res = con.execute(prod_query_8)
fetched_results = res.fetchall()

y_m_array = ['-'.join((str(result[0]), str(result[1]),)) for result in fetched_results]
single_unit = [result[2] for result in fetched_results]
multi_unit = [result[3] for result in fetched_results]

fig8 = make_subplots()
fig8.add_trace(
    go.Scatter(x=y_m_array, y=single_unit, name="single_unit"),
    secondary_y=False,
)
fig8.add_trace(
    go.Scatter(x=y_m_array, y=multi_unit, name="multi_unit"),
    secondary_y=False,
)
fig8.update_xaxes(title_text="year and month")
fig8.update_yaxes(title_text="<b>primary</b> loans by number of units ownership %", secondary_y=False)
fig8.show()


# plotly 
res = con.execute(prod_query_6)
fetched_results = res.fetchall()

y_m_array = ['-'.join((str(result[0]), str(result[1]),)) for result in fetched_results]
first_time_buyer = [result[2] for result in fetched_results]
second_time_buyer = [result[3] for result in fetched_results]
fed_total_purchases = [result[4] for result in fetched_results]
fed_mbs_purchases = [result[5] for result in fetched_results]

fig6 = make_subplots(specs=[[{"secondary_y": True}]])
fig6.add_trace(
    go.Scatter(x=y_m_array, y=first_time_buyer, name="first_time_buyer"),
    secondary_y=False,
)
fig6.add_trace(
    go.Scatter(x=y_m_array, y=second_time_buyer, name="not first_time buyer"),
    secondary_y=False,
)

fig6.update_layout(
    title_text="first time buyer % vs not first time %"
)
fig6.update_xaxes(title_text="year and month")
fig6.update_yaxes(title_text="<b>primary</b> buyers by prior ownership %", secondary_y=False)
fig6.show()
"""
"""
res = con.execute(occupancy_upb_vs_fed_purchases)
fetched_results = res.fetchall()

y_m_array = ['-'.join((str(result[0]), str(result[1]),)) for result in fetched_results]
primary_residence_array = [result[2] for result in fetched_results]
secondary_residence_array = [result[3] for result in fetched_results]
investment_property_array = [result[4] for result in fetched_results]
unknown_upb = [result[5] for result in fetched_results]
fig7 = make_subplots()
fig7.add_trace(
    go.Scatter(x=y_m_array, y=primary_residence_array, name="primary_residence_fraction"),
    secondary_y=False,
)
fig7.add_trace(
    go.Scatter(x=y_m_array, y=secondary_residence_array, name="secondary_residence"),
    secondary_y=False,
)
fig7.add_trace(
    go.Scatter(x=y_m_array, y=investment_property_array, name="investment_property_fraction"),
    secondary_y=False,
)
fig7.add_trace(
    go.Scatter(x=y_m_array, y=unknown_upb, name="unknown_upb_fraction"),
    secondary_y=False,
)

fig7.update_layout(
    title_text="investment property upb and primary residence upb on same axis"
)
fig7.update_xaxes(title_text="upbs compared")
fig7.update_yaxes(title_text="<b>primary</b> yaxis title", secondary_y=False)
fig7.show()
"""