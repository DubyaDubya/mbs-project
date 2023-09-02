from issuance_migration import *

class Loan_Issuance_File_Transformation(Issuance_File_Transformation):
    """a class instantiated for each Issuance file in a dataset. Reads files from S3, 
    changes date columns into proper date format, then saves to the dataset
    Therefore, this dataset does not need to be partitioned"""
    
    def __init__(self, file_system: fs.FileSystem, csv_path: str, table: pa.Table):
        date_columns = ["First Payment Date", "Maturity Date", "Interest Only First Principal and Interest Payment Date",
                             "Next Interest Rate Adjustment Date", "Terminal Step Date", 
                             "Next Step Rate Adjustment Date", "Origination First Payment Date", "Origination Maturity Date"]
        Issuance_File_Transformation.__init__(self, file_system, csv_path, table, date_columns)

    def _add_issuance_columns(self):
        #add a column for the issuance year
        #add a column for the issuance month
        no_ext = self.csv_path.split('.')
        path_split = no_ext[0].split('/')
        fn = path_split[-1]
        date_str = fn.split('_')[-1]
        year = int(date_str[:4])
        month = int(date_str[4:])
        years = pa.array([year] * self.table.num_rows)
        months = pa.array([month] * self.table.num_rows)

        self.table = self.table.add_column(0, "Issuance_Year", years)
        self.table = self.table.add_column(1, "Issuance_Month", months)

    def final_table(self):
        self._change_date_columns_to_string()
        self._change_date_columns_string_to_proper_format()
        self._change_date_columns_to_date()
        self._add_issuance_columns()
        self._drop_filler_columns()
        return self.table

    #pq.write_table(table)
    #pq.write_table(final_table, 'william-mbs-data/fanniemae/parquet/issuance-data.parquet', filesystem=s3)

class Loan_Issuance_Files_Migration:

    def __init__(self, file_system, from_folder, to_path):
        self.file_system = file_system
        self.to_path = to_path
        csv_info = self.file_system.get_file_info(fs.FileSelector(from_folder))
        self.partitioning = ds.partitioning(
        pa.schema([("Issuance_Year", pa.int64()), ("Issuance_Month", pa.int64())]), 
        flavor="hive")
        self.from_paths = [fi.path for fi in csv_info]

    def __call__(self):
        for path in self.from_paths:
            issuance_table = Loan_Issuance_File_Transformation(self.file_system, path, CSV_Converter(self.file_system, path)()).final_table()
            ds.write_dataset(issuance_table, self.to_path, format='parquet',
                             partitioning=self.partitioning, filesystem=self.file_system, existing_data_behavior='overwrite_or_ignore')
            print(f'read and wrote {path}')


    

if __name__ == '__main__':
    s3 = fs.S3FileSystem(access_key='***REMOVED***',
                     secret_key='***REMOVED***',
                     region='***REMOVED***')
    
    lm = Loan_Issuance_Files_Migration(s3, '***REMOVED***', '***REMOVED***')
    lm()