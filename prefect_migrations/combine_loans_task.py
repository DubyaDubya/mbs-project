from pyarrow import fs as fs, dataset as ds, parquet as pq
import pyarrow as pa
from prefect import task
from final_loan_schema import final_schema


def year_month_set_parquet(file_system, path):
    folder_info = file_system.get_file_info(fs.FileSelector(path,recursive=True))
    parquet_file_paths = [fi.path for fi in folder_info if fi.is_file]
    split_final_paths = [path.split('/') for path in parquet_file_paths]
    year_month_parquet = {(int(path[-3].split('=')[1]), int(path[-2].split('=')[1]),) for path in split_final_paths}
    return year_month_parquet

#find all the (Year,Month,) combos in destination
#find all (Year,Month,) combos in fannie mae folder
#find all (Year,Month,) combos in freddie mac folder

#for all year month combos in fannie mae and not in destination folder, add to final list
#for all year month combos in freddie mac and not in destination folder, add to final list
#for each year month combo in final list, read and concat the associated files from both datasets
#write dataset


@task
def combine_and_save_loans(file_system, from_fannie, from_freddie, final_destination, partition,log_prints=True):

    fannie_set = year_month_set_parquet(file_system, from_fannie)
    print(f"fannie mae full folders: {fannie_set}")

    freddie_set = year_month_set_parquet(file_system, from_freddie)
    print(f"freddie mac full folders: {fannie_set}")

    final_set = year_month_set_parquet(file_system, final_destination)
    need_to_be_migrated = set()
    for t in fannie_set:
        if t not in final_set:
            need_to_be_migrated.add(t)
    for t in freddie_set:
        if t not in final_set:
            need_to_be_migrated.add(t)
    
    print(f"need to be migrated {need_to_be_migrated}")

    fannie_mae_dataset = ds.dataset(from_fannie, filesystem=file_system, partitioning=partition, schema=final_schema)
    freddie_mac_dataset = ds.dataset(from_freddie, filesystem=file_system, partitioning=partition, schema=final_schema)
    

    for year, month in need_to_be_migrated:
        year = pa.scalar(year, type=pa.int64())
        month = pa.scalar(month, type=pa.int64())

        fannie_table = fannie_mae_dataset.to_table(filter=((ds.field('Issuance_Year') == year) & (ds.field('Issuance_Month') == month)))
        print(f'fannie year: {year} month: {month} works')
        freddie_table = freddie_mac_dataset.to_table(filter=((ds.field('Issuance_Year') == year) & (ds.field('Issuance_Month') == month)))
        print(f'freddie year: {year} month: {month} works')
        if fannie_table.schema.equals(freddie_table.schema):
            print(f'both schemas read the same')
        else:
            print(f'schemas are different')
            break
        final_table = pa.concat_tables([fannie_table, freddie_table])

        ds.write_dataset(final_table, final_destination, format='parquet',
                         partitioning=partition, filesystem=file_system, existing_data_behavior='overwrite_or_ignore')
        #print(f'read and wrote data from {path}')