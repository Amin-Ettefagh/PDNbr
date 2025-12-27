import os
import re
import math
import unicodedata
from io import BytesIO

import pandas as pd
import msoffcrypto
from sqlalchemy import create_engine, text
from tqdm import tqdm

from Config import CONFIG



class DataNormalizer:

    CHAR_MAP = {
        ord("ي"): "ی",
        ord("ك"): "ک",
        ord("ى"): "ی",
        ord("ة"): "ه",
        ord("ۀ"): "ه",
        ord("ؤ"): "و",
        ord("إ"): "ا",
        ord("أ"): "ا",
        ord("آ"): "ا",

        ord("۰"): "0", ord("۱"): "1", ord("۲"): "2", ord("۳"): "3", ord("۴"): "4",
        ord("۵"): "5", ord("۶"): "6", ord("۷"): "7", ord("۸"): "8", ord("۹"): "9",

        ord("٠"): "0", ord("١"): "1", ord("٢"): "2", ord("٣"): "3", ord("٤"): "4",
        ord("٥"): "5", ord("٦"): "6", ord("٧"): "7", ord("٨"): "8", ord("٩"): "9",
    }

    @staticmethod
    def normalize_text(v):
        if v is None or pd.isna(v):
            return None

        s = str(v)
        s = unicodedata.normalize("NFKC", s)
        s = s.translate(DataNormalizer.CHAR_MAP)

        s = "".join(
            ch for ch in s
            if unicodedata.category(ch)[0] != "C" or ch in "\r\n\t"
        )

        s = s.strip()
        if s == "" or s.lower() == "null":
            return None
        return s

    @staticmethod
    def normalize_numeric(v):
        s = DataNormalizer.normalize_text(v)
        if s is None:
            return None

        s = s.replace("٬", "").replace("٫", ".")
        s = re.sub(r"[^\d\.-]", "", s)
        return None if s == "" else s

    @staticmethod
    def cast_series(series, sql_type):
        t = sql_type.upper()

        if t in ("INT", "BIGINT"):
            return series.apply(
                lambda x: pd.to_numeric(
                    DataNormalizer.normalize_numeric(x),
                    errors="coerce"
                )
            )

        if t.startswith("NVARCHAR"):
            m = re.search(r"\((\d+|MAX)\)", t)
            max_len = None if not m or m.group(1) == "MAX" else int(m.group(1))

            def f(x):
                v = DataNormalizer.normalize_text(x)
                if v is None:
                    return None
                return v if max_len is None else v[:max_len]

            return series.astype(object).apply(f)

        return series.astype(object).apply(DataNormalizer.normalize_text)





class ConfigDataLoader:

    def __init__(self, batch_size=200_000, max_params=2000, bad_records_path="bad.txt"):
        self.cfg = CONFIG
        self.batch_size = batch_size
        self.max_params = max_params
        self.bad_records_path = bad_records_path
        self.engine = None

    # ---------------- DB Connection ----------------
    def build_engine(self):
        c = self.cfg
        driver = c["pyodbc_driver"]
        server = c["server_ip"]
        db = c["database_name"]

        if c["auth_method"] == "WinAuth":
            conn = f"Driver={driver};Server={server};Database={db};Trusted_Connection=yes;"
        else:
            conn = f"Driver={driver};Server={server};Database={db};UID={c['username']};PWD={c['password']};"

        from urllib.parse import quote_plus
        self.engine = create_engine(
            "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn),
            fast_executemany=True
        )

        with self.engine.connect() as con:
            con.execute(text("SELECT 1"))

    # ---------------- Excel Reader ----------------
    def read_excel(self, path, sheet, passwords):
        with open(path, "rb") as f:
            of = msoffcrypto.OfficeFile(f)
            if not of.is_encrypted():
                return pd.read_excel(path, sheet_name=sheet, dtype=str)

        for p in passwords:
            try:
                with open(path, "rb") as f:
                    of = msoffcrypto.OfficeFile(f)
                    of.load_key(password=p)
                    bio = BytesIO()
                    of.decrypt(bio)
                    bio.seek(0)
                    return pd.read_excel(bio, sheet_name=sheet, dtype=str)
            except Exception:
                continue

        raise RuntimeError("Invalid Excel password")

    # ---------------- CSV / TXT ----------------
    def read_text_batches(self, path, encoding, delimiter):
        return pd.read_csv(
            path,
            sep=delimiter,
            encoding=encoding,
            dtype=str,
            engine="python",
            keep_default_na=False,
            chunksize=self.batch_size
        )

    # ---------------- Schema Apply ----------------
    def build_df(self, df, table_def):
        out = {}
        for f in table_def["fields"]:
            name = f["name"]
            typ = f["type"]

            if name in df.columns:
                out[name] = DataNormalizer.cast_series(df[name], typ)
            else:
                out[name] = [None] * len(df)

        return pd.DataFrame(out)

    # ---------------- Insert ----------------
    def safe_insert(self, df, table_name):
        if df is None or df.empty:
            return
        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
            method=None
        )

    # ---------------- MAIN ----------------
    def load_all(self):
        self.build_engine()

        for table_name, tdef in self.cfg["tables"].items():
            fp = tdef["input"]["file"]
            sheet = tdef["input"]["sheet"]
            encoding = tdef["encoding"]
            delimiter = tdef["delimiter"]

            ext = os.path.splitext(fp)[1].lower()
            print(f"▶ Loading {table_name}")

            if ext in (".xlsx", ".xls"):
                df_raw = self.read_excel(fp, sheet, self.cfg.get("file_password", []))
                df = self.build_df(df_raw, tdef)
                self.safe_insert(df, table_name)
                print(f"  ✔ {len(df)} rows inserted")
                continue

            for chunk in self.read_text_batches(fp, encoding, delimiter):
                df = self.build_df(chunk, tdef)
                self.safe_insert(df, table_name)

            print("  ✔ Done")






loader = ConfigDataLoader(
    batch_size=200000,
    max_params=2000,
    bad_records_path=r"D:\Data\bad_records.txt"
)
loader.load_all()
