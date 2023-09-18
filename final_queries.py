prod_queries = []
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
prod_queries.append(loan_ints_vs_fed_purchases)

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
prod_queries.append(upb_and_fed_purchases)

upb_by_channel_vs_purchases = """WITH totals AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN Channel = 'R' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS retail_upb,
SUM(CASE WHEN Channel = 'C' THEN "Issuance Investor Loan UPB" ELSE 0 END ) AS correspondent_upb,
SUM(CASE WHEN Channel = 'B' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS broker_upb,
SUM(CASE WHEN Channel = '' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS unknown_upb,
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
broker_upb, unknown_upb, total_upb FROM fed_purchases
JOIN totals ON fed_purchases.Year = totals.Issuance_Year AND fed_purchases.Month = totals.Issuance_Month
ORDER BY Year, Month ASC"""
prod_queries.append(upb_by_channel_vs_purchases)

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

SELECT Year, Month, cash_out_refi, no_cash_refi, purchase, modified_loss_mitigation, total,  total_purchases, mbs_purchases
FROM upb_by_purpose JOIN fed_purchases ON upb_by_purpose.Issuance_Year = fed_purchases.Year AND upb_by_purpose.Issuance_Month = fed_purchases.Month
ORDER BY Year, Month ASC"""

prod_queries.append(purpose_upb_vs_fed_purchases)

cashout_vs_fed_purchases = """WITH upb_by_purpose AS (SELECT Issuance_Year, Issuance_Month,
SUM("Issuance Investor Loan UPB") AS cash_out_refi,
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
FROM upb_by_purpose JOIN fed_purchases ON upb_by_purpose.Issuance_Year = fed_purchases.Year AND upb_by_purpose.Issuance_Month = fed_purchases.Month
ORDER BY Year, Month ASC"""
prod_queries.append(cashout_vs_fed_purchases)

first_time_upb_vs_fed_purchases = """WITH upb_by_first_time AS (SELECT Issuance_Year, Issuance_Month,
SUM(CASE WHEN "First Time Home Buyer Indicator" = 'Y' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS first_time_upb,
SUM(CASE WHEN "First Time Home Buyer Indicator" = 'N' THEN "Issuance Investor Loan UPB" ELSE 0 END) AS not_first_time_upb,
first_time_upb + not_first_time_upb AS total
FROM loan_data_set l
GROUP BY Issuance_Year, Issuance_Month),

upb_ft_frac AS (SELECT Issuance_Year, Issuance_Month, first_time_upb / total AS first_time_upb_frac, 
not_first_time_upb / total AS not_first_time_frac,
first_time_upb, not_first_time_upb FROM upb_by_first_time),

fed_holdings AS (SELECT Year, Month, AVG(Total) total_holdings, AVG(MBS) mbs_holdings FROM fed_data_set f
WHERE "As Of Date" >= '2019-06-01'
GROUP BY Year, Month
ORDER BY Year, Month ASC),

fed_purchases AS (SELECT Year, Month, total_holdings - LAG(total_holdings, 1, 3.66635423794372e+12) OVER () AS total_purchases,
mbs_holdings - LAG(mbs_holdings,1,1.568056830344e+12) OVER () AS mbs_purchases
FROM fed_holdings)

SELECT Year, Month, first_time_upb_frac, not_first_time_frac, first_time_upb, not_first_time_upb,
total_purchases, mbs_purchases
FROM fed_purchases JOIN upb_ft_frac ON fed_purchases.Year = upb_ft_frac.Issuance_Year 
AND fed_purchases.Month = upb_ft_frac.Issuance_Month
ORDER BY Year, Month ASC"""
prod_queries.append(first_time_upb_vs_fed_purchases)

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
AND fed_purchases.Month = upb_occupancy_frac.Issuance_Month
ORDER BY Year, Month ASC"""
prod_queries.append(occupancy_upb_vs_fed_purchases)

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
AND fed_purchases.Month = upb_units_frac.Issuance_Month
ORDER BY Year, Month ASC"""
prod_queries.append(number_of_units_vs_fed_purchases)