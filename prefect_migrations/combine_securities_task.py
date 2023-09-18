from pyarrow import fs as fs, parquet as pq
import pyarrow as pa
from prefect import task

@task
def combine_and_save(filesystem, from_fannie, from_freddie, final_destination):
    fannie_table = pq.read_table(from_fannie, filesystem=filesystem)
    freddie_table = pq.read_table(from_freddie, filesystem=filesystem)
    full_table = pa.concat_tables([fannie_table, freddie_table], promote=True)
    pq.write_table(full_table, final_destination, filesystem=filesystem)

