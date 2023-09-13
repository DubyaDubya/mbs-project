import pyarrow as pa
from pyarrow import dataset as ds, fs, compute as cp, parquet as pq
from prefect_migrations.table_from_csv_task import csv_to_table
from prefect import task

#Don't really need to partition issuance files
#Do need to partition Loan Level files, and need to partition them by Issuance date (year/month)
#of security who's CUSIP they're associated with

@task
def drop_filler_columns(table: pa.Table, log_prints=True):
    while (len(table.schema.get_all_field_indices("Filler")) != 0):
            table =table.remove_column(table.schema.get_all_field_indices("Filler")[0])
    print("all filler columns dropped")
    return table

def _find_date_column_indices(table: pa.Table, date_columns):
    schema = table.schema
    return [(col_name, schema.get_field_index(col_name)) for col_name in date_columns]

def _find_date_column_indices(table, date_columns):
        schema = table.schema
        return [(col_name, schema.get_field_index(col_name)) for col_name in date_columns]

@task
def change_date_columns_to_string(table: pa.Table, date_columns, log_prints=True):
    for col_id in _find_date_column_indices(table, date_columns):
        column = table.column(col_id[1])
        if not pa.types.is_null(column.type):
            # cast to string
            casted_col = cp.cast(column, pa.string())
            #find maximum length of columns
            lengths = cp.utf8_length(casted_col)
            max_len = cp.max(lengths).as_py()
            #if its odd, that means its  missing a leading 0 before a month 
            if (max_len % 2 == 1):
                max_len = max_len + 1
            lengths_padded = cp.utf8_lpad(casted_col,max_len,'0')
            table = table.set_column(col_id[1], col_id[0], lengths_padded)
            print(f"{col_id[0]} updated to string")
    return table

@task
def change_date_columns_string_to_proper_format(table, date_columns, log_prints=True):
    for col_id in _find_date_column_indices(table, date_columns):
        print(f"updating {col_id[0]} to date string")
        column = table.column(col_id[1])
        if not pa.types.is_null(column.type):
            #find maximum length of columns
            max_len = cp.max(cp.utf8_length(column)).as_py()
            new_lis = None
            if max_len == 6:
                new_lis = pa.array([None if st is None else st[:2] + "/01/" + st[2:] for st in column.to_pylist()])
            if max_len == 8:
                new_lis = pa.array([None if st is None else st[:2] + "/" + st[2:4] + "/" + st[4:] for st in column.to_pylist() if st is not None])
            table = table.set_column(col_id[1], col_id[0], new_lis)
            print(f"{col_id[0]} updated to date string")
    return table
                
@task
def change_date_columns_to_date(table: pa.Table, date_columns, log_prints=True):
    #need to find all date columns
    #need to check if its a 6 char date column or an 8 char date column
    #for each date column, compute utf8 length and then compute min and max
    #if min and max are same for all, we're good. Otherwise bad news
    for col_id in _find_date_column_indices(table, date_columns):
        column = table.column(col_id[1])
        if not pa.types.is_null(column.type):
            dt_arr = cp.strptime(column, "%m/%d/%Y", "ms")
            table = table.set_column(col_id[1], col_id[0], dt_arr)
            print(f"{col_id[0]} updated to date")
    return table
