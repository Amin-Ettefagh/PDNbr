import os
import json
import csv
import sys
import pandas as pd
import re

# ---------------- CSV FIELD SIZE LIMIT ----------------
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int //= 10


class FileSchemaExtractor:

    ENCODINGS = [
        "utf-8-sig", "utf-8",
        "cp1256", "cp1252",
        "iso-8859-6", "iso-8859-1",
        "utf-16", "utf-16-le", "utf-16-be"
    ]

    DELIMITERS = [",", "|", ";", "\t", "~"]

    def __init__(self, folder_path: str):
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
        lines = open(file_path, encoding=encoding, errors="ignore").read().splitlines()
        scores = {}

        for d in self.DELIMITERS:
            counts = [len(row) for row in csv.reader(lines, delimiter=d)]
            scores[d] = len(set(counts))

        return min(scores, key=scores.get)

    # ---------------- Normalize ----------------
    def normalize_columns(self, df):
        df.columns = [
            c if isinstance(c, str) and str(c).strip() else f"column{i+1}"
            for i, c in enumerate(df.columns)
        ]
        return df

    def normalize_dataframe(self, df):
        rows = df.values.tolist()
        max_cols = max(len(r) for r in rows)

        for i in range(len(df.columns), max_cols):
            df[f"column{i+1}"] = None

        normalized = []
        for r in rows:
            if len(r) < max_cols:
                r = r + [None] * (max_cols - len(r))
            normalized.append(r)

        return pd.DataFrame(normalized, columns=df.columns)

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

    # ---------------- Build Table ----------------
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

    # ---------------- Process ----------------
    def process_text(self, path):
        encoding = self.detect_encoding(path)
        delimiter = self.detect_delimiter(path, encoding)

        df = pd.read_csv(
            path,
            sep=delimiter,
            dtype=str,
            encoding=encoding,
            engine="python"
        )

        df = self.normalize_columns(df)
        df = self.normalize_dataframe(df)

        return df, encoding, delimiter

    def process_excel(self, path):
        xls = pd.ExcelFile(path)
        data = {}
        for sheet in xls.sheet_names:
            df = pd.read_excel(path, sheet_name=sheet, dtype=str)
            df = self.normalize_columns(df)
            df = self.normalize_dataframe(df)
            data[sheet] = df
        return data

    # ---------------- Run ----------------
    def run(self, output_path):
        if not os.path.isdir(self.folder_path):
            raise RuntimeError(f"Folder not accessible: {self.folder_path}")

        for root, _, files in os.walk(self.folder_path):
            for f in files:
                full = os.path.join(root, f)
                ext = f.lower().split(".")[-1]

                if ext in ("csv", "txt"):
                    df, enc, delim = self.process_text(full)
                    table_name = os.path.splitext(f)[0]

                    self.tables[table_name] = self.build_table(
                        df, full, "null", enc, delim
                    )

                elif ext == "xlsx":
                    sheets = self.process_excel(full)
                    for sheet, df in sheets.items():
                        table_name = f"{os.path.splitext(f)[0]}_{sheet}"
                        self.tables[table_name] = self.build_table(
                            df, full, sheet, "null", "null"
                        )

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"tables": self.tables}, f, ensure_ascii=False, indent=2)


# ---------------- USAGE ----------------
# extractor = FileSchemaExtractor(r"\\192.168.1.10\SharedData")
# extractor.run(r"\\192.168.1.10\SharedData\output.json")
