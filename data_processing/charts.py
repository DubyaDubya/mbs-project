import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pyarrow import parquet as pq, fs
import pyarrow as pa
from settings import aws_access_key, aws_secret_key, aws_region, final_folder
from typing import List, Any
from collections.abc import Mapping


s3 = fs.S3FileSystem(access_key=aws_access_key,
                     secret_key=aws_secret_key,
                     region=aws_region)

data_table1 = pq.read_table(final_folder + "/query-0.parquet", filesystem=s3)

def y_m_array(table: pa.Table, year_col_name: str, month_col_name: str) -> List[str]:
    years = table.column('Year').to_pylist()
    months = table.column('Month').to_pylist()
    return ['-'.join((str(year),str(months[i]),)) for i, year in enumerate(years)]

y_m_array1 = y_m_array(data_table1, 'Year', 'Month')

w_avg_int_array = [w_avg_int for w_avg_int in data_table1.column('w_avg_int').to_pylist()]
total_purchases = [total_purchases for total_purchases in data_table1.column('total_purchases').to_pylist()]
mbs_purchases = [mbs_purchases for mbs_purchases in data_table1.column('mbs_purchases').to_pylist()]


def dual_axis_chart(x_array: List[str], y_arrays_axis1 : Mapping[str, List[Any]], y_arrays_axis2 : Mapping[str, List[Any]],
                    x_axis_title: str, y_axes_titles: List[str], title: str) -> go.Figure:
    plot = make_subplots(specs=[[{"secondary_y": True}]])
    for entry in y_arrays_axis1.keys():
        plot.add_trace(
            go.Scatter(x=x_array, y=y_arrays_axis1[entry], name=entry),
            secondary_y=False
        )
    for entry in y_arrays_axis2.keys():
        plot.add_trace(
            go.Scatter(x=x_array, y=y_arrays_axis2[entry], name=entry),
            secondary_y=True
        )
    plot.update_layout(title_text=title)
    plot.update_xaxes(title_text=x_axis_title)
    plot.update_yaxes(title_text=y_axes_titles[0], secondary_y=False)
    plot.update_yaxes(title_text=y_axes_titles[1], secondary_y=True)
    return plot


#data1 weighted average interest rate vs fed net purchases
y1_arrays = {"weighted average loan interest rate": w_avg_int_array}
y2_arrays = {"fed monthly balance sheet change": total_purchases,
             "fed monthly MBS balance sheet change": mbs_purchases}
title_text = "weighted average interest rate plotted with fed balance sheet changes"
x_title = "year and month"
y_titles = ["Interest Rate (%)", "Fed Balance sheet monthly increase (Trillions of dollars)"]
plot1 = dual_axis_chart(y_m_array1, y1_arrays, y2_arrays, x_title, y_titles, title_text)

#data2 issuance_upb vs fed net purchases
data_table2 = pq.read_table(final_folder + "/query-1.parquet", filesystem=s3)
y_m_2 = y_m_array(data_table2, 'Year', 'Month')
total_issuance_upb = [upb for upb in data_table2.column('total_issuance_upb').to_pylist()]
y1_arrays = {}
y2_arrays["total UPB of newly issued pools"] = total_issuance_upb
title_text = "total UPB of newly issued fannie mae pools plotted next to fed balance sheet changes in the same month"
y_titles[1] = "Trillions of dollars"
plot2 = dual_axis_chart(y_m_array1, y1_arrays, y2_arrays, x_title, y_titles, title_text)


#need to figure out plotly express/ pandas dependency issue for this one 
data_table3 = pq.read_table(final_folder + "/query-2.parquet", filesystem=s3)
wide_df = data_table3.to_pandas()
wide_df["y_m"] = wide_df['Year'].astype(str) +"-"+ wide_df["Month"].astype(str)
wide_df["retail_upb_pct"] = 100 * wide_df["retail_upb"]/ wide_df["total_upb"]
wide_df["correspondent_upb_pct"] = 100 * wide_df["correspondent_upb"]/ wide_df["total_upb"]
wide_df["broker_upb_pct"] = 100 * wide_df["broker_upb"]/ wide_df["total_upb"]
wide_df["unknown_upb_pct"] = 100 * wide_df["unknown_upb"]/ wide_df["total_upb"]

plot3 = px.bar(wide_df, x="y_m", y=["retail_upb_pct", "correspondent_upb_pct", "broker_upb_pct", "unknown_upb_pct"],
                title="Total UPB by Loan Source")

#
data_table4 = pq.read_table(final_folder + "/query-3.parquet", filesystem=s3)
wide_df = data_table4.to_pandas()
wide_df["y_m"] = wide_df['Year'].astype(str) +"-"+ wide_df["Month"].astype(str)
wide_df["cash_out_refi_pct"] = 100 * wide_df["cash_out_refi"]/ wide_df["total"]
wide_df["no_cash_refi_pct"] = 100 * wide_df["no_cash_refi"]/ wide_df["total"]
wide_df["purchase_pct"] = 100 * wide_df["purchase"]/ wide_df["total"]
wide_df["modified_loss_mitigation_pct"] = 100 * wide_df["modified_loss_mitigation"]/ wide_df["total"]
plot4 = px.bar(wide_df, x="y_m", y=["cash_out_refi", "no_cash_refi","purchase","modified_loss_mitigation"],
                title="Total UPB by Loan Purpose")
plot5 = px.line(wide_df, x="y_m", y=["cash_out_refi"],
                title="UPB of Cash Out Refinances Over Time")

plot6 = px.line(wide_df, x="y_m", y=["purchase"],
                title="UPB of Purchases Over Time")
plot6.update_yaxes(range=[0, 90_000_000_000])
plot7 = px.line(wide_df, x="y_m", y=["no_cash_refi"],
                title="UPB of Non-cash Refinances Over Time")

data_table5 = pq.read_table(final_folder + "/query-5.parquet", filesystem=s3)
wide_df=data_table5.to_pandas()
wide_df["y_m"] = wide_df['Year'].astype(str) +"-"+ wide_df["Month"].astype(str)

plot8 = px.line(wide_df, x="y_m", y=["first_time_upb", "not_first_time_upb"],
                title="Wide-Form Input")

data_table6 = pq.read_table(final_folder +'/query-6.parquet', filesystem=s3)
wide_df=data_table6.to_pandas()
wide_df["y_m"] = wide_df['Year'].astype(str) +"-"+ wide_df["Month"].astype(str)
plot9 = px.bar(wide_df, x="y_m", y=['primary_residence_fraction', 'secondary_residence_fraction',
                                    'investment_property_fraction', 'unknown_fraction'])

data_table7 = pq.read_table(final_folder + '/query-7.parquet', filesystem=s3)
wide_df=data_table7.to_pandas()
wide_df["y_m"] = wide_df['Year'].astype(str) +"-"+ wide_df["Month"].astype(str)
plot10 = px.bar(wide_df, x="y_m", y=['single_unit_fraction', 'multi_unit_fraction'])

data_table8 = pq.read_table(final_folder + '/query-8.parquet', filesystem=s3)
print(data_table8)
'''
wide_df = data_table8.to_pandas()
wide_df["Year_Month"] = wide_df['Issuance_Year'].astype(str) +"-"+ wide_df["Issuance_Month"].astype(str)
wide_df["Average Loan Term"] = wide_df['AVG("Loan Term")']
plot11 = px.bar(wide_df, x="Year_Month", y=['Average Loan Term'])

plot11.show()
'''

plot1.show()
plot2.show()
plot3.show()
plot4.show()
plot5.show()
plot6.show()
plot7.show()
plot8.show()
plot9.show()
plot10.show()