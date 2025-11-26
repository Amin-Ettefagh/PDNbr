import os
import pandas as pd
from tqdm import tqdm
import json

class FileStructureExtractor:
    def __init__(self, paths):
        self.paths = paths if isinstance(paths, list) else [paths]
        self.tables = {}

    def detect_type(self, series):
        s = series.dropna()
        if s.empty:
            return "NVARCHAR(MAX)"
        try:
            s.astype(int)
            return "INT"
        except:
            pass
        try:
            pd.to_datetime(s, errors="raise")
            return "DATETIME"
        except:
            pass
        max_len = s.astype(str).apply(len).max()
        return f"NVARCHAR({max_len})" if max_len < 4000 else "NVARCHAR(MAX)"

    def process_txt(self, path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(2048)
        delimiter = "," if sample.count(",") >= sample.count("\t") else "\t"
        return pd.read_csv(path, delimiter=delimiter)

    def process_csv(self, path):
        return pd.read_csv(path)

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

    def run(self):
        results = {}
        pbar = tqdm(self.paths, desc="Processing files", ncols=100)

        for path in pbar:
            ext = os.path.splitext(path)[1].lower()
            if ext == ".xlsx":
                sheets = self.process_excel(path)
                for sheet_name, df in sheets.items():
                    table_name = self.generate_table_name(path, sheet_name)
                    self.tables[table_name] = {
                        "schema": "",
                        "input": {
                            "file": path,
                            "sheet": sheet_name
                        },
                        "fields": []
                    }
                    df.columns = [c if isinstance(c, str) else f"Col_{i+1}" for i, c in enumerate(df.columns)]
                    for col in df.columns:
                        sql_type = self.detect_type(df[col])
                        self.tables[table_name]["fields"].append({
                            "file": col,
                            "db": "",
                            "type": sql_type
                        })

            elif ext == ".csv":
                df = self.process_csv(path)
                table_name = self.generate_table_name(path)
                self.tables[table_name] = {
                    "schema": "",
                    "input": {
                        "file": path,
                        "sheet": None
                    },
                    "fields": []
                }
                df.columns = [c if isinstance(c, str) else f"Col_{i+1}" for i, c in enumerate(df.columns)]
                for col in df.columns:
                    sql_type = self.detect_type(df[col])
                    self.tables[table_name]["fields"].append({
                        "file": col,
                        "db": "",
                        "type": sql_type
                    })

            elif ext == ".txt":
                df = self.process_txt(path)
                table_name = self.generate_table_name(path)
                self.tables[table_name] = {
                    "schema": "",
                    "input": {
                        "file": path,
                        "sheet": None
                    },
                    "fields": []
                }
                df.columns = [c if isinstance(c, str) else f"Col_{i+1}" for i, c in enumerate(df.columns)]
                for col in df.columns:
                    sql_type = self.detect_type(df[col])
                    self.tables[table_name]["fields"].append({
                        "file": col,
                        "db": "",
                        "type": sql_type
                    })

        if os.path.exists("C.txt"):
            os.remove("C.txt")

        with open("C.txt", "w", encoding="utf-8") as f:
            json.dump({"tables": self.tables}, f, indent=4)

        return {"tables": self.tables}





paths = [
    r"D:\Data\Users.xlsx",
    r"D:\Data\Products.csv",
    r"D:\Data\Logs.txt"
]

extractor = FileStructureExtractor(paths)
result = extractor.run()

print(result)


