from prefect_migrations.issuance_task import drop_filler_columns, change_date_columns_to_string, change_date_columns_string_to_proper_format, change_date_columns_to_date, _find_date_column_indices
from prefect_migrations.table_from_csv_task import csv_to_table
from pyarrow import compute as cp, parquet as pq, fs
import pyarrow as pa
from prefect import task, flow


date_columns = ['Security Factor Date', 'Issue Date', 'Maturity Date',
                             'First Rate Adjustment Date', 'First Payment Adjustment Date',
                             'Next Step Rate Adjustment Date']

@task
def security_add_issuance_columns(table, log_prints=True):
    col_id = 'Issue Date'
    column = table.column(col_id)
    year = cp.year(column)
    month = cp.month(column)
    table = table.add_column(0, "Issuance_Year", year)
    print("Issuance_Year column added")
    table = table.add_column(1, "Issuance_Month", month)
    print("Issuance_Month column added")
    return table

@flow(name="Security Transformation", log_prints=True)
def security_issuance_transformation(table):
    table = change_date_columns_to_string(table, date_columns)
    table = change_date_columns_string_to_proper_format(table, date_columns)
    table = change_date_columns_to_date(table, date_columns)
    table = security_add_issuance_columns(table, date_columns)
    table = drop_filler_columns(table)
    print("table transformation complete complete")
    return table


@flow(name="Security Migration", log_prints=True)
def security_issuance_migration(file_system, from_folder, to_path):
    csv_info = file_system.get_file_info(fs.FileSelector(from_folder))
    from_paths = [fi.path for fi in csv_info]
    issuance_table = pa.concat_tables([security_issuance_transformation(csv_to_table(
        file_system, path)) for path in from_paths], promote=True)
    print("issuance table combined")
    pq.write_table(issuance_table, to_path, filesystem=file_system)
    print(f"issuance table written to {to_path}")