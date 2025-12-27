import os
import json
import csv
import pandas as pd
import re
from collections import defaultdict


class FileSchemaExtractor:

    ENCODINGS = [
        "utf-8-sig", "utf-8",
        "cp1256", "cp1252",
        "iso-8859-6", "iso-8859-1",
        "utf-16", "utf-16-le", "utf-16-be"
    ]

    DELIMITERS = [",", "|", ";", "\t", "~"]

    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.tables = {}

    # ---------------- Encoding ----------------
    def detect_encoding(self, file_path):
        raw = open(file_path, "rb").read()
        best, best_score = None, -1

        for enc in self.ENCODINGS:
            try:
                text = raw.decode(enc)
            except Exception:
                continue

            bad = text.count("\ufffd")
            ctrl = sum(1 for c in text if ord(c) < 9)
            score = len(text) - bad * 1000 - ctrl * 10

            if score > best_score:
                best, best_score = enc, score

        return best or "utf-8"

    # ---------------- Delimiter ----------------
    def detect_delimiter(self, file_path, encoding):
        scores = {}
        lines = open(file_path, encoding=encoding, errors="ignore").read().splitlines()

        for d in self.DELIMITERS:
            counts = [len(row) for row in csv.reader(lines, delimiter=d)]
            scores[d] = len(set(counts))

        return min(scores, key=scores.get)

    # ---------------- Normalize ----------------
    def normalize_dataframe(self, df):
        max_cols = max(len(row) for row in df.values.tolist())
        for i in range(len(df.columns), max_cols):
            df[f"column{i+1}"] = None
        return df

    def normalize_columns(self, df):
        df.columns = [
            c if isinstance(c, str) and c.strip() else f"column{i+1}"
            for i, c in enumerate(df.columns)
        ]
        return df

    # ---------------- Type Detection ----------------
    def nvarchar_size(self, ln):
        if ln < 25: return "NVARCHAR(25)"
        if ln < 50: return "NVARCHAR(50)"
        if ln < 75: return "NVARCHAR(75)"
        if ln < 100: return "NVARCHAR(100)"
        if ln < 200: return "NVARCHAR(200)"
        return "NVARCHAR(MAX)"

    def detect_type(self, values):
        clean = [v for v in values if v not in (None, "", "null")]

        if not clean:
            return "NVARCHAR(25)"

        int_ok, float_ok = True, True
        max_len, max_int = 0, 0

        for v in clean:
            s = str(v)
            max_len = max(max_len, len(s))

            if not re.fullmatch(r"-?\d+", s):
                int_ok = False
            else:
                max_int = max(max_int, abs(int(s)))

            if not re.fullmatch(r"-?\d+(\.\d+)?", s):
                float_ok = False

        if int_ok:
            return "INT" if max_int <= 2147483647 else "BIGINT"
        if float_ok:
            return "FLOAT"

        return self.nvarchar_size(max_len)

    # ---------------- Process ----------------
    def process_text(self, path):
        encoding = self.detect_encoding(path)
        delimiter = self.detect_delimiter(path, encoding)
        df = pd.read_csv(path, sep=delimiter, dtype=str, encoding=encoding, engine="python")
        return df, encoding, delimiter

    def process_excel(self, path):
        xls = pd.ExcelFile(path)
        return {s: pd.read_excel(path, sheet_name=s, dtype=str) for s in xls.sheet_names}

    # ---------------- Run ----------------
    def run(self, output_path):
        for root, _, files in os.walk(self.folder_path):
            for f in files:
                full = os.path.join(root, f)
                ext = f.lower().split(".")[-1]

                if ext in ("csv", "txt"):
                    df, enc, delim = self.process_text(full)
                    df = self.normalize_columns(self.normalize_dataframe(df))

                    table = os.path.splitext(f)[0]
                    self.tables[table] = self.build_table(
                        df, full, "null", enc, delim
                    )

                elif ext == "xlsx":
                    sheets = self.process_excel(full)
                    for sheet, df in sheets.items():
                        df = self.normalize_columns(self.normalize_dataframe(df))
                        table = f"{os.path.splitext(f)[0]}_{sheet}"
                        self.tables[table] = self.build_table(
                            df, full, sheet, "null", "null"
                        )

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"tables": self.tables}, f, ensure_ascii=False, indent=2)

    def build_table(self, df, file_path, sheet, encoding, delimiter):
        fields = []
        for col in df.columns:
            fields.append({
                "name": str(col),
                "type": self.detect_type(df[col].tolist()),
                "nullable": "true"
            })

        return {
            "encoding": str(encoding),
            "delimiter": str(delimiter),
            "rows_checked": str(len(df)),
            "input": {
                "file": str(file_path),
                "sheet": str(sheet)
            },
            "fields": fields
        }




extractor = FileSchemaExtractor(r"D:\Data")
extractor.run(r"D:\Data\output.json")
