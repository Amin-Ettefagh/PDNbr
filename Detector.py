import os
import json
import warnings
import datetime as dt

import pandas as pd
from tqdm import tqdm


class FileStructureExtractor:
    def __init__(self, paths, sample_rows=5000):
        self.paths = paths if isinstance(paths, list) else [paths]
        self.sample_rows = sample_rows
        self.tables = {}
        warnings.filterwarnings("ignore", category=UserWarning)

    def detect_type(self, series):
        s = series.dropna()
        if s.empty:
            return "NVARCHAR(25)"

        if len(s) > self.sample_rows:
            s = s.sample(self.sample_rows, random_state=0)

        s_str = s.astype(str)

        # Detect numeric
        num = pd.to_numeric(s_str, errors="coerce")
        num_non_null = num.dropna()
        if not num_non_null.empty and len(num_non_null) / len(s_str) >= 0.8:
            if ((num_non_null % 1) == 0).all():
                max_val = num_non_null.abs().max()
                try:
                    max_val_float = float(max_val)
                except Exception:
                    max_val_float = None
                if max_val_float is not None:
                    if max_val_float <= 32767:
                        return "SMALLINT"
                    elif max_val_float <= 2147483647:
                        return "INT"
                    elif max_val_float <= 9223372036854775807:
                        return "BIGINT"
                    else:
                        return "DECIMAL(38,0)"
                return "DECIMAL(38,0)"
            return "DECIMAL(18,4)"

        # Detect date/datetime
        dt_parsed = pd.to_datetime(s_str, errors="coerce", infer_datetime_format=True)
        dt_non_null = dt_parsed.dropna()
        if not dt_non_null.empty and len(dt_non_null) / len(s_str) >= 0.8:
            times = dt_non_null.dt.time
            if all(t == dt.time(0, 0) for t in times):
                return "DATE"
            return "DATETIME2(0)"

        # TEXT → Always NVARCHAR with ranges
        lengths = s_str.str.len()
        max_len = int(lengths.max())

        if max_len <= 0:
            max_len = 1

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
        if max_len <= 300:
            return "NVARCHAR(300)"

        return "NVARCHAR(MAX)"

    def process_txt(self, path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(2048)
        delimiter = "," if sample.count(",") >= sample.count("\t") else "\t"
        return pd.read_csv(path, delimiter=delimiter, nrows=self.sample_rows)

    def process_csv(self, path):
        return pd.read_csv(path, nrows=self.sample_rows)

    def process_excel(self, path):
        xls = pd.ExcelFile(path)
        data = {}
        for sheet in xls.sheet_names:
            data[sheet] = pd.read_excel(path, sheet_name=sheet, nrows=self.sample_rows)
        return data

    def generate_table_name(self, path, sheet=None):
        base = os.path.splitext(os.path.basename(path))[0]
        if sheet:
            return f"{base}_{sheet}"
        return base

    def normalize_columns(self, df):
        new_cols = []
        for i, c in enumerate(df.columns):
            if isinstance(c, str) and c.strip() != "":
                new_cols.append(c)
            else:
                new_cols.append(f"Col_{i+1}")
        df.columns = new_cols
        return df

    def build_fields_with_progress(self, df, desc):
        fields = []
        cols = list(df.columns)
        with tqdm(total=len(cols), desc=desc, ncols=100) as pbar:
            for col in cols:
                sql_type = self.detect_type(df[col])
                fields.append(
                    {
                        "file": col,
                        "db": "",
                        "type": sql_type,
                    }
                )
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
                    desc = f"{os.path.basename(path)} | {sheet_name}"
                    fields = self.build_fields_with_progress(df, desc)
                    self.tables[table_name] = {
                        "schema": "",
                        "input": {
                            "file": path,
                            "sheet": sheet_name,
                        },
                        "fields": fields,
                    }
            elif ext == ".csv":
                df = self.process_csv(path)
                df = self.normalize_columns(df)
                table_name = self.generate_table_name(path)
                desc = os.path.basename(path)
                fields = self.build_fields_with_progress(df, desc)
                self.tables[table_name] = {
                    "schema": "",
                    "input": {
                        "file": path,
                        "sheet": None,
                    },
                    "fields": fields,
                }
            elif ext == ".txt":
                df = self.process_txt(path)
                df = self.normalize_columns(df)
                table_name = self.generate_table_name(path)
                desc = os.path.basename(path)
                fields = self.build_fields_with_progress(df, desc)
                self.tables[table_name] = {
                    "schema": "",
                    "input": {
                        "file": path,
                        "sheet": None,
                    },
                    "fields": fields,
                }

        if os.path.exists(output_path):
            os.remove(output_path)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"tables": self.tables}, f, ensure_ascii=False, indent=4)
