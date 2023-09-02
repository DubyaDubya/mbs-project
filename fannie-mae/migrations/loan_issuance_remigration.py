### files are pretty small (5-20MB) want to remigrate to the year level
import pyarrow as pa
from pyarrow import dataset as ds, fs, parquet as pq


s3 = fs.S3FileSystem(access_key='***REMOVED***',
                     secret_key='***REMOVED***',
                     region='***REMOVED***')

read_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")

write_part = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64())]), 
        flavor="hive")




csv_info = s3.get_file_info(fs.FileSelector('***REMOVED***', recursive=True))
paths = [f.path for f in csv_info if f.is_file]



#need to make one coherent schema
schemas = [pq.read_schema(path, filesystem=s3) for path in paths]
full_schema = schemas[0]
for schema in schemas:
    for name in full_schema.names:
        field1 = full_schema.field(name)
        field2 = schema.field(name)
        if not field1.equals(field2):
            if pa.types.is_null(field1.type):
                full_schema = full_schema.set(full_schema.get_field_index(name),field2)

full_schema = full_schema.append(pa.field('Issuance_Year', pa.int64()))
full_schema = full_schema.append(pa.field('Issuance_Month', pa.int64()))

read_data_set = ds.dataset('***REMOVED***', schema=full_schema, format='parquet', filesystem=s3, partitioning=read_part)

def fv(written_file):
    print(f'wrote {written_file.path}')

years = [2019, 2020, 2021, 2022, 2023]
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
for year in years:
        for month in months:
             bs = read_data_set.to_batches(filter=((ds.field('Issuance_Year') == year) & (ds.field('Issuance_Month') == month)))
             print(f'read_in {year}, {month}')
             ds.write_dataset(bs, '***REMOVED***2', schema=full_schema, format='parquet', filesystem=s3, partitioning=read_part, file_visitor=fv, existing_data_behavior='delete_matching')


#ds.write_dataset(read_data_set, '***REMOVED***2', format='parquet', filesystem=s3, partitioning=write_part, existing_data_behavior='delete_matching')

#print(full_schema)
#s2 = read_data_set.scanner().dataset_schema
#print(s1)
#print(s2)
#print(s1.equals(s2))
#doesnt work, hangs for like 20 minutes 