from pyarrow import csv, parquet as pq
from pyarrow import fs

class CSV_Converter:
    
    def __init__(self, fs: fs.S3FileSystem, csv_path):
        self.fs = fs
        self.csv_path = csv_path

        

    def open_csv(self):
        with self.fs.open_input_stream(self.csv_path) as csv_f:
            return csv.read_csv(csv_f, parse_options=csv.ParseOptions(delimiter='|'))

    def __call__(self):
        t = self.open_csv()
        return t

