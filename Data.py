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
                display(Markdown(f"### 🔌 Connecting ({attempt}/{max_retries})"))
                self.engine = create_engine(url, fast_executemany=True)
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                display(Markdown("### ✅ Connection successful"))
                return True
            except Exception as e:
                display(Markdown(f"### ❌ Connection failed:\n```\n{str(e)}\n```"))
                if attempt >= max_retries:
                    display(Markdown("### ⛔ Giving up"))
                    return False
                time.sleep(delay_seconds)
        return False

    def decrypt_excel(self, file_path, sheet):
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
                    display(Markdown(f"### 🔓 Decrypted `{file_path}`"))
                    return df
            except Exception:
                continue
        display(Markdown(f"### ❌ Wrong password for `{file_path}`, skipped"))
        return None

    def read_csv_enc(self, file_path):
        encodings = self.cfg.get("encoding") or ["utf-8"]
        last_error = None
        for enc in encodings:
            try:
                return pd.read_csv(file_path, encoding=enc)
            except Exception as e:
                last_error = e
                continue
        raise last_error

    def read_txt_enc(self, file_path):
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
        raise last_error

    def read_file(self, file_path, sheet=None):
        ext = os.path.splitext(file_path)[1].lower()
        if ext in [".xls", ".xlsx", ".xlsm", ".xlsb"]:
            return self.decrypt_excel(file_path, sheet)
        if ext == ".csv":
            return self.read_csv_enc(file_path)
        if ext == ".txt":
            return self.read_txt_enc(file_path)
        raise ValueError(f"Unsupported extension: {ext}")

    def cast_series(self, series, type_str):
        t = type_str.upper()
        if t in ("SMALLINT", "INT", "BIGINT"):
            return pd.to_numeric(series, errors="coerce").astype("Int64")
        if t == "DECIMAL(38,0)":
            return pd.to_numeric(series, errors="coerce").astype("Int64")
        if t == "DECIMAL(18,4)":
            return pd.to_numeric(series, errors="coerce")
        if t == "DATE":
            return pd.to_datetime(series, errors="coerce").dt.date
        if t.startswith("DATETIME"):
            return pd.to_datetime(series, errors="coerce")
        if t.startswith("NVARCHAR"):
            return series.astype(object).apply(lambda x: "" if pd.isna(x) else str(x))
        return series

    def build_dataframe(self, df, table_def):
        fields = table_def["fields"]
        out = {}
        for f in fields:
            src = f["file"]
            dst = f["db"]
            t = f["type"]
            if not dst:
                continue
            if src not in df.columns:
                continue
            out[dst] = self.cast_series(df[src], t)
        if not out:
            return pd.DataFrame()
        return pd.DataFrame(out)

    def split_batches(self, df):
        total = len(df)
        if total <= self.batch_size:
            return [df]
        batches = []
        count = math.ceil(total / self.batch_size)
        for i in range(count):
            s = i * self.batch_size
            e = min(s + self.batch_size, total)
            batches.append(df.iloc[s:e])
        return batches

    def load_all(self):
        if not self.build_engine_with_retry():
            return

        for table_name, table_def in self.cfg["tables"].items():
            file_path = table_def["input"]["file"]
            sheet_name = table_def["input"]["sheet"]
            schema = table_def.get("schema") or None

            display(Markdown(f"---\n## ▶️ Loading into **{table_name}**"))
            display(Markdown(f"`{file_path}`"))

            if not os.path.exists(file_path):
                display(Markdown(f"### ❌ File not found"))
                continue

            try:
                df_raw = self.read_file(file_path, sheet_name)
                if df_raw is None:
                    continue
            except Exception as e:
                display(Markdown(f"### ❌ Read error:\n```\n{str(e)}\n```"))
                continue

            df_mapped = self.build_dataframe(df_raw, table_def)
            total_rows = len(df_mapped)
            if total_rows == 0:
                display(Markdown("### ⚠ No rows"))
                continue

            batches = self.split_batches(df_mapped)
            total_batches = len(batches)

            with tqdm(total=total_batches, desc=f"{os.path.basename(file_path)} → {table_name}", ncols=100, unit="batch") as pbar:
                for i, b in enumerate(batches, start=1):
                    try:
                        b.to_sql(
                            name=table_name,
                            con=self.engine,
                            schema=schema,
                            if_exists="append",
                            index=False,
                            method="multi",
                            chunksize=10_000
                        )
                    except Exception as e:
                        display(Markdown(f"### ❌ Insert error batch {i}:\n```\n{str(e)}\n```"))
                        break
                    pbar.update(1)

            display(Markdown(f"### ✅ Finished `{table_name}` ({total_rows} rows)"))

        display(Markdown("# 🎉 Completed all loads"))




# loader = ConfigDataLoader(batch_size=1_000_000)
# loader.load_all()
