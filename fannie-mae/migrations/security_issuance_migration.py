from migrations.issuance_migration import *

class Security_Issuance_File_Transformation(Issuance_File_Transformation):
    """a class instantiated for each Security Issuance file in a dataset. Reads files from S3, 
    changes date columns into proper date format, then saves to the dataset
    Therefore, this dataset does not need to be partitioned"""
    
    def __init__(self, file_system: fs.FileSystem, csv_path: str, table: pa.Table):
        date_columns = ['Security Factor Date', 'Issue Date', 'Maturity Date',
                             'First Rate Adjustment Date', 'First Payment Adjustment Date',
                             'Next Step Rate Adjustment Date']
        Issuance_File_Transformation.__init__(self, file_system, csv_path, table, date_columns)
        
    def _add_issuance_columns(self):
        #add a column for the issuance year
        #add a column for the issuance month
        col_id = self._find_date_column_indices()[1]
        column = self.table.column(col_id[1])
        year = cp.year(column)
        month = cp.month(column)
        self.table = self.table.add_column(0, "Issuance_Year", year)
        self.table = self.table.add_column(1, "Issuance_Month", month)

    def final_table(self):
        self._change_date_columns_to_string()
        self._change_date_columns_string_to_proper_format()
        self._change_date_columns_to_date()
        self._add_issuance_columns()
        self._drop_filler_columns()
        return self.table

class Security_Issuance_Files_Migration:
    def __init__(self, file_system, from_folder, to_path):
        self.file_system = file_system
        self.to_path = to_path
        csv_info = self.file_system.get_file_info(fs.FileSelector(from_folder))
        self.from_paths = [fi.path for fi in csv_info]

    def __call__(self):
        issuance_table = pa.concat_tables([Security_Issuance_File_Transformation(
            self.file_system, path, CSV_Converter(self.file_system, path)()).final_table()
            for path in self.from_paths], promote=True)
        pq.write_table(issuance_table, self.to_path, filesystem=self.file_system)
