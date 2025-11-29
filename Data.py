import os
import math
import time
from io import BytesIO

import msoffcrypto
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from IPython.display import display, Markdown
from Config import CONFIG


class ConfigDataLoader:
    def __init__(self, batch_size=1_000_000):
        self.cfg = CONFIG
        self.engine = None
        self.batch_size = batch_size

    def build_engine_with_retry(self, max_retries=5, delay_seconds=2):
        driver = self.cfg["pyodbc_driver"]
        server = self.cfg["server_ip"]
        database = self.cfg["database_name"]
        username = self.cfg["username"]
        password = self.cfg["password"]
        auth = self.cfg["auth_method"]

        if auth == "WinAuth":
            conn_str = (
                f"Driver={driver};"
                f"Server={server};"
                f"Database={database};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"Driver={driver};"
                f"Server={server};"
                f"Database={database};"
                f"UID={username};"
                f"PWD={password};"
            )

        from urllib.parse import quote_plus
        url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)

        attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                display(Markdown(f"### 🔌 Trying to connect (attempt {attempt}/{max_retries})"))
                self.engine = create_engine(url, fast_executemany=True)
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                display(Markdown("### ✅ Connection test passed."))
                return True
            except SQLAlchemyError as e:
                display(Markdown(f"### ❌ Connection failed:\n```\n{str(e)}\n```"))
                if attempt >= max_retries:
                    display(Markdown("### ⛔ Maximum retry attempts reached. Aborting."))
                    return False
                time.sleep(delay_seconds)
        return False

    def read_excel_maybe_encrypted(self, file_path, sheet):
        passwords = self.cfg.get("file_password") or []
        with open(file_path, "rb") as f:
            of = msoffcrypto.OfficeFile(f)
            if not of.is_encrypted():
                return pd.read_excel(file_path, sheet_name=sheet if sheet else 0, engine="openpyxl")
        for pwd in passwords:
            try:
                with open(file_path, "rb") as f:
                    of = msoffcrypto.OfficeFile(f)
                    if not of.is_encrypted():
                        return pd.read_excel(file_path, sheet_name=sheet if sheet else 0, engine="openpyxl")
                    of.load_key(password=pwd)
                    bio = BytesIO()
                    of.decrypt(bio)
                    bio.seek(0)
                    df = pd.read_excel(bio, sheet_name=sheet if sheet else 0, engine="openpyxl")
                    display(Markdown(f"### 🔓 Decrypted Excel `{file_path}` with password."))
                    return df
            except Exception:
                continue
        display(Markdown(f"### ❌ Failed to decrypt Excel file `{file_path}` with provided passwords. Skipping table."))
        return None

    def read_csv_with_encodings(self, file_path):
        encodings = self.cfg.get("encoding") or ["utf-8"]
        last_error = None
        for enc in encodings:
            try:
                return pd.read_csv(file_path, encoding=enc)
            except Exception as e:
                last_error = e
                continue
        raise last_error if last_error else ValueError("No encoding worked for CSV.")

    def read_txt_with_encodings(self, file_path):
        encodings = self.cfg.get("encoding") or ["utf-8"]
        last_error = None
        for enc in encodings:
            try:
                with open(file_path, "r", encoding=enc, errors="strict") as f:
                    sample = f.read(2048)
                delimiter = "," if sample.count(",") >= sample.count("\t") else "\t"
                return pd.read_csv(file_path, delimiter=delimiter, encoding=enc)
            except Exception as e:
                last_error = e
                continue
        raise last_error if last_error else ValueError("No encoding worked for TXT.")

    def read_file(self, file_path, sheet=None):
        ext = os.path.splitext(file_path)[1].lower()
        if ext in [".xls", ".xlsx", ".xlsm", ".xlsb"]:
            return self.read_excel_maybe_encrypted(file_path, sheet)
        if ext == ".csv":
            return self.read_csv_with_encodings(file_path)
        if ext == ".txt":
            return self.read_txt_with_encodings(file_path)
        raise ValueError(f"Unsupported file type: {ext}")

    def cast_series(self, series, type_str):
        t = type_str.upper()
        s = series
        if t in ("SMALLINT", "INT", "BIGINT", "DECIMAL(38,0)", "DECIMAL(18,4)"):
            return pd.to_numeric(s, errors="coerce")
        if t == "DATE":
            return pd.to_datetime(s, errors="coerce").dt.date
        if t.startswith("DATETIME"):
            return pd.to_datetime(s, errors="coerce")
        if t.startswith("NVARCHAR"):
            return s.astype(str)
        return s

    def map_and_cast_dataframe(self, df, table_def):
        fields = table_def["fields"]
        data = {}
        for f in fields:
            src = f["file"]
            dst = f["db"]
            t = f["type"]
            if not dst:
                continue
            if src not in df.columns:
                continue
            col = df[src]
            data[dst] = self.cast_series(col, t)
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)

    def split_batches(self, df):
        total_rows = len(df)
        if total_rows == 0:
            return []
        if total_rows <= self.batch_size:
            return [df]
        batches = []
        total_batches = math.ceil(total_rows / self.batch_size)
        for i in range(total_batches):
            start = i * self.batch_size
            end = min(start + self.batch_size, total_rows)
            batches.append(df.iloc[start:end])
        return batches

    def load_all(self):
        if not self.build_engine_with_retry():
            return

        for table_name, table_def in self.cfg["tables"].items():
            file_path = table_def["input"]["file"]
            sheet_name = table_def["input"]["sheet"]
            schema = table_def.get("schema") or None

            display(Markdown(f"---\n## ▶️ Loading data for table **{table_name}**"))
            display(Markdown(f"**Source file:** `{file_path}`  \n**Sheet:** `{sheet_name}`"))

            if not os.path.exists(file_path):
                display(Markdown(f"### ❌ File not found: `{file_path}`"))
                continue

            try:
                df_raw = self.read_file(file_path, sheet_name)
                if df_raw is None:
                    continue
            except Exception as e:
                display(Markdown(f"### ❌ Error reading file `{file_path}`:\n```\n{str(e)}\n```"))
                continue

            df_mapped = self.map_and_cast_dataframe(df_raw, table_def)
            total_rows = len(df_mapped)

            if total_rows == 0:
                display(Markdown("### ⚠ No rows to insert after mapping."))
                continue

            batches = self.split_batches(df_mapped)
            total_batches = len(batches)

            pbar_desc = f"{os.path.basename(file_path)} → {table_name}"
            with tqdm(total=total_batches, desc=pbar_desc, ncols=100, unit="batch") as pbar:
                for i, batch_df in enumerate(batches, start=1):
                    try:
                        batch_df.to_sql(
                            name=table_name,
                            con=self.engine,
                            schema=schema,
                            if_exists="append",
                            index=False,
                            method="multi",
                            chunksize=10_000
                        )
                    except SQLAlchemyError as e:
                        display(Markdown(f"### ❌ Error inserting batch {i}/{total_batches} for `{table_name}`:\n```\n{str(e)}\n```"))
                        break
                    pbar.update(1)

            display(Markdown(f"### ✅ Finished loading table `{table_name}` ({total_rows} rows)."))
        display(Markdown("# 🎉 All data load tasks completed."))






# loader = ConfigDataLoader(batch_size=1_000_000)
# loader.load_all()
