import os
import json
import warnings
import pandas as pd
from tqdm import tqdm


class FileStructureExtractor:
    def __init__(self, paths, delimiter=",", copy_file_to_db_name=False):
        self.paths = paths if isinstance(paths, list) else [paths]
        self.delimiter = delimiter
        self.copy_file_to_db_name = copy_file_to_db_name
        self.tables = {}
        warnings.filterwarnings("ignore", category=UserWarning)

    def detect_type(self, series):
        s = series.dropna().astype(str)
        if s.empty:
            return "NVARCHAR(25)"

        is_numeric = s.str.isdigit()
        no_leading_zero = ~s.str.startswith("0") | (s == "0")

        if is_numeric.all() and no_leading_zero.all():
            max_len = s.str.len().max()
            if max_len <= 9:
                return "INT"
            return "BIGINT"

        lengths = s.str.len()
        max_len = int(lengths.max())

        if max_len <= 25:
            return "NVARCHAR(25)"
        if max_len <= 50:
            return "NVARCHAR(50)"
        if max_len <= 75:
            return "NVARCHAR(75)"
        if max_len <= 100:
            return "NVARCHAR(100)"
        if max_len <= 200:
            return "NVARCHAR(200)"
        return "NVARCHAR(MAX)"

    def process_csv(self, path):
        return pd.read_csv(
            path,
            sep=self.delimiter,
            engine="python",
            on_bad_lines="skip"
        )

    def process_txt(self, path):
        return pd.read_csv(
            path,
            sep=self.delimiter,
            engine="python",
            on_bad_lines="skip"
        )

    def process_excel(self, path):
        xls = pd.ExcelFile(path)
        data = {}
        for sheet in xls.sheet_names:
            data[sheet] = pd.read_excel(path, sheet_name=sheet)
        return data

    def generate_table_name(self, path, sheet=None):
        base = os.path.splitext(os.path.basename(path))[0]
        if sheet:
            return f"{base}_{sheet}"
        return base

    def normalize_columns(self, df):
        cols = []
        for i, c in enumerate(df.columns):
            if isinstance(c, str) and c.strip():
                cols.append(c)
            else:
                cols.append(f"Col_{i+1}")
        df.columns = cols
        return df

    def build_fields_with_progress(self, df, desc):
        fields = []
        with tqdm(total=len(df.columns), desc=desc, ncols=100) as pbar:
            for col in df.columns:
                t = self.detect_type(df[col])
                fields.append({
                    "file": col,
                    "db": col if self.copy_file_to_db_name else "",
                    "type": t
                })
                pbar.update(1)
        return fields

    def run(self, output_path="C.txt"):
        for path in self.paths:
            ext = os.path.splitext(path)[1].lower()

            if ext == ".xlsx":
                sheets = self.process_excel(path)
                for sheet_name, df in sheets.items():
                    df = self.normalize_columns(df)
                    table_name = self.generate_table_name(path, sheet_name)
                    fields = self.build_fields_with_progress(df, f"{os.path.basename(path)} | {sheet_name}")
                    self.tables[table_name] = {
                        "schema": "",
                        "collation": "SQL_Latin1_General_CP1256_CI_AS",
                        "input": {"file": path, "sheet": sheet_name},
                        "fields": fields
                    }

            elif ext == ".csv":
                df = self.process_csv(path)
                df = self.normalize_columns(df)
                table_name = self.generate_table_name(path)
                fields = self.build_fields_with_progress(df, os.path.basename(path))
                self.tables[table_name] = {
                    "schema": "",
                    "collation": "SQL_Latin1_General_CP1256_CI_AS",
                    "input": {"file": path, "sheet": "null"},
                    "fields": fields
                }

            elif ext == ".txt":
                df = self.process_txt(path)
                df = self.normalize_columns(df)
                table_name = self.generate_table_name(path)
                fields = self.build_fields_with_progress(df, os.path.basename(path))
                self.tables[table_name] = {
                    "schema": "",
                    "collation": "SQL_Latin1_General_CP1256_CI_AS",
                    "input": {"file": path, "sheet": "null"},
                    "fields": fields
                }

        if os.path.exists(output_path):
            os.remove(output_path)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"tables": self.tables}, f, ensure_ascii=False, indent=4)


extractor = FileStructureExtractor(
    paths=[
        r"D:\Data\Users.csv",
        r"D:\Data\Orders.txt",
        r"D:\Data\Products.xlsx"
    ],
    delimiter=",",
    copy_file_to_db_name=True
)

extractor.run(r"D:\Data\schema_output.json")

