import requests
from pyarrow import csv
import pyarrow as pa
from pyarrow import csv, compute as cp, fs, parquet as pq




class Fed_Holdings_Migration:
    
    def __init__(self, file_system, to_path):
        self.url = 'https://markets.newyorkfed.org/api/soma/summary.csv'
        self.file_system = file_system
        self.to_path = to_path

    def request_data(self):
        resp = requests.get(self.url)
        return resp

    def get_pure_table(self, resp):
        buf = pa.py_buffer(resp.content)
        with pa.input_stream(buf) as st:
            t1 = csv.read_csv(st)
        return t1
    
    def add_year_month(self,table):
        date = table.column(0)
        year = cp.year(date)
        month = cp.month(date)
        table = table.add_column(0, "Year", year)
        table = table.add_column(1, "Month", month)
        return table
    
    def write(self, table):
        #keeping it as a csv_file
        with self.file_system.open_output_stream(self.to_path) as out_stream:
            csv.write_csv(table, out_stream)

    
    def __call__(self):
        resp = self.request_data()
        t = self.get_pure_table(resp)
        t = self.add_year_month(t)
        self.write(t)
        

