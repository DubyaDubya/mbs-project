from prefect import task, flow
import pyarrow as pa
from pyarrow import fs, dataset as ds
from prefect_migrations.issuance_task import change_date_columns_to_string, change_date_columns_string_to_proper_format, change_date_columns_to_date, drop_filler_columns
from prefect_migrations.table_from_csv_task import csv_to_table


date_columns = ["First Payment Date", "Maturity Date", "Interest Only First Principal and Interest Payment Date",
                             "Next Interest Rate Adjustment Date", "Terminal Step Date", 
                             "Next Step Rate Adjustment Date", "Origination First Payment Date", "Origination Maturity Date"]

def get_csv_year_month(csv_path):
    no_ext = csv_path.split('.')
    path_split = no_ext[0].split('/')
    fn = path_split[-1]
    date_str = fn.split('_')[-1]
    year = int(date_str[:4])
    month = int(date_str[4:])
    return (year, month,)

@task
def loan_add_issuance_columns(table, csv_path, log_prints=True):
    year, month = get_csv_year_month(csv_path)
    years = pa.array([year] * table.num_rows)
    months = pa.array([month] * table.num_rows)
    table = table.add_column(0, "Issuance_Year", years)
    print("Issuance_Year column added")
    table = table.add_column(1, "Issuance_Month", months)
    print("Issuance_Month column added")
    return table

@flow(name="Loan Transformation", log_prints=True)
def loan_issuance_transformation(table, csv_path):
    table = change_date_columns_to_string(table, date_columns)
    table = change_date_columns_string_to_proper_format(table, date_columns)
    table = change_date_columns_to_date(table, date_columns)
    table = loan_add_issuance_columns(table, csv_path)
    table = drop_filler_columns(table)
    print(f"loan issuance tranformation complete for {csv_path}")
    return table
    
@flow(name="Loan Migration", log_prints=True)
def loan_issuance_migration(file_system, from_folder, to_path, loan_partitioning):
    csv_info = file_system.get_file_info(fs.FileSelector(from_folder))
    from_paths = [fi.path for fi in csv_info]
    year_months_csv = [get_csv_year_month(path) for path in from_paths]
    year_months_str = ['/'.join((str(year_month[0]), str(year_month[1]),)) for year_month in year_months_csv]
    csv_ym_to_path = {year_month: from_paths[i] for i, year_month in enumerate(year_months_str)}
    print("got csv paths")

    parquet_info = file_system.get_file_info(fs.FileSelector(to_path,recursive=True))
    final_file_paths = [fi.path for fi in parquet_info if fi.is_file]
    split_final_paths = [path.split('/') for path in final_file_paths]
    year_month_parquet = {'/'.join(((path[-3].split('=')[1]), path[-2].split('=')[1],)) for path in split_final_paths}
    csvs_not_transferred = [csv_ym_to_path[ym] for ym in year_months_str if ym not in year_month_parquet]
    print(csvs_not_transferred)

    for path in csvs_not_transferred:
        issuance_table = loan_issuance_transformation(csv_to_table(file_system, path), path)
        ds.write_dataset(issuance_table, to_path, format='parquet',
                         partitioning=loan_partitioning, filesystem=file_system, existing_data_behavior='overwrite_or_ignore')
        print(f'read and wrote data from {path}')
    print('finished writing loan data set')