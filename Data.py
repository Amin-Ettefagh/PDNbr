import os
import math
import time
from io import BytesIO
import msoffcrypto
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text
from IPython.display import display, Markdown
from Config import CONFIG


def normalize_numeric(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    s = s.replace("۰", "0").replace("۱", "1").replace("۲", "2").replace("۳", "3").replace("۴", "4")
    s = s.replace("۵", "5").replace("۶", "6").replace("۷", "7").replace("۸", "8").replace("۹", "9")
    s = s.replace("٬", ",").replace("٫", ".")
    s = s.replace("\u200c", "").replace("\u200f", "")
    if s in ["", "-", "—", "_"]:
        return None
    return s


class ConfigDataLoader:
    def __init__(self, batch_size=1_000_000, max_params=2000, separator=","):
        self.cfg = CONFIG
        self.engine = None
        self.batch_size = batch_size
        self.max_params = max_params
        self.separator = separator or ","

    def build_engine_with_retry(self, max_retries=5, delay_seconds=2):
        driver = self.cfg["pyodbc_driver"]
        server = self.cfg["server_ip"]
        database = self.cfg["database_name"]
        username = self.cfg["username"]
        password = self.cfg["password"]
        auth = self.cfg["auth_method"]
        if auth == "WinAuth":
            conn_str = f"Driver={driver};Server={server};Database={database};Trusted_Connection=yes;"
        else:
            conn_str = f"Driver={driver};Server={server};Database={database};UID={username};PWD={password};"
        from urllib.parse import quote_plus
        url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)
        for _ in range(max_retries):
            try:
                self.engine = create_engine(url, fast_executemany=True)
                with self.engine.connect() as c:
                    c.execute(text("SELECT 1"))
                display(Markdown("### ✅ Connection successful"))
                return True
            except Exception:
                time.sleep(delay_seconds)
        display(Markdown("### ❌ Failed to connect"))
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
                    of.load_key(password=pwd)
                    bio = BytesIO()
                    of.decrypt(bio)
                    bio.seek(0)
                    return pd.read_excel(bio, sheet_name=sheet if sheet else 0, engine="openpyxl")
            except Exception:
                continue
        return None

    def read_csv(self, file_path):
        encs = self.cfg.get("encoding") or ["utf-8"]
        for e in encs:
            try:
                return pd.read_csv(
                    file_path,
                    encoding=e,
                    sep=self.separator,
                    engine="python",
                    on_bad_lines="skip"
                )
            except Exception:
                continue
        return None

    def read_txt(self, file_path):
        encs = self.cfg.get("encoding") or ["utf-8"]
        for e in encs:
            try:
                return pd.read_csv(
                    file_path,
                    encoding=e,
                    sep=self.separator,
                    engine="python",
                    on_bad_lines="skip"
                )
            except Exception:
                continue
        return None

    def read_file(self, fp, sheet):
        ext = os.path.splitext(fp)[1].lower()
        if ext in [".xlsx", ".xls", ".xlsm", ".xlsb"]:
            return self.decrypt_excel(fp, sheet)
        if ext == ".csv":
            return self.read_csv(fp)
        if ext == ".txt":
            return self.read_txt(fp)
        return None

    def cast_series(self, s, t):
        t = t.upper()
        if t in ("SMALLINT", "INT", "BIGINT", "DECIMAL(38,0)"):
            return s.apply(lambda x: pd.to_numeric(normalize_numeric(x), errors="coerce")).astype("Int64")
        if t == "DECIMAL(18,4)":
            return s.apply(lambda x: pd.to_numeric(normalize_numeric(x), errors="coerce"))
        if t == "DATE":
            return pd.to_datetime(s, errors="coerce").dt.date
        if t.startswith("DATETIME"):
            return pd.to_datetime(s, errors="coerce")
        if t.startswith("NVARCHAR"):
            return s.astype(object).apply(lambda x: "" if pd.isna(x) else str(x))
        return s

    def build_df(self, df, table):
        out = {}
        for f in table["fields"]:
            src = f["file"]
            dst = f["db"]
            t = f["type"]
            if src in df and dst:
                out[dst] = self.cast_series(df[src], t)
        return pd.DataFrame(out)

    def split_batches(self, df):
        total = len(df)
        if total <= self.batch_size:
            return [df]
        res = []
        c = math.ceil(total / self.batch_size)
        for i in range(c):
            s = i * self.batch_size
            e = min(s + self.batch_size, total)
            res.append(df.iloc[s:e])
        return res

    def safe_insert(self, df, table_name, schema):
        meta = self.cfg["tables"][table_name]["fields"]
        limits = {}
        for f in meta:
            t = f["type"].upper()
            if t.startswith("NVARCHAR(") and "MAX" not in t:
                limits[f["db"]] = int(t[t.find("(") + 1:t.find(")")])

        for col, lim in limits.items():
            if col in df:
                df = df[df[col].astype(str).str.len() <= lim]

        if df.empty:
            return

        cols = len(df.columns)
        safe_rows = max(1, self.max_params // cols)
        total = len(df)
        parts = math.ceil(total / safe_rows)

        for i in range(parts):
            s = i * safe_rows
            e = min(s + safe_rows, total)
            df.iloc[s:e].to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists="append",
                index=False,
                method=None
            )

    def load_all(self):
        if not self.build_engine_with_retry():
            return
        for table_name, tdef in self.cfg["tables"].items():
            fp = tdef["input"]["file"]
            sheet = tdef["input"]["sheet"]
            schema = tdef.get("schema")
            if not os.path.exists(fp):
                continue
            df_raw = self.read_file(fp, sheet)
            if df_raw is None:
                continue
            df = self.build_df(df_raw, tdef)
            if df.empty:
                continue
            batches = self.split_batches(df)
            for b in batches:
                self.safe_insert(b, table_name, schema)
        display(Markdown("# 🎉 Completed"))





loader = ConfigDataLoader(separator="|")
loader.load_all()
