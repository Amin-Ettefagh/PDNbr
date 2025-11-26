import json
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.types import (
    Integer, SmallInteger, BigInteger, String,
    Date, DateTime, Numeric
)
from sqlalchemy.exc import SQLAlchemyError
from IPython.display import display, Markdown
from Config import CONFIG


class DatabaseBuilder:
    def __init__(self):
        self.cfg = CONFIG
        self.engine = None

    def build_engine(self):
        driver = self.cfg["pyodbc_driver"]
        server = self.cfg["server_ip"]
        database = self.cfg["database_name"]
        username = self.cfg["username"]
        password = self.cfg["password"]
        auth = self.cfg["auth_method"]

        if auth == "WinAuth":
            conn = (
                f"Driver={driver};"
                f"Server={server};"
                f"Database={database};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn = (
                f"Driver={driver};"
                f"Server={server};"
                f"Database={database};"
                f"UID={username};"
                f"PWD={password};"
            )

        from urllib.parse import quote_plus
        conn_url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn)

        try:
            self.engine = create_engine(conn_url, fast_executemany=True)
            with self.engine.connect() as conn_test:
                conn_test.execute(text("SELECT 1"))
            display(Markdown("### ✅ Connection test passed."))
        except SQLAlchemyError as e:
            display(Markdown(f"### ❌ Connection failed:\n```\n{str(e)}\n```"))
            raise e

    def map_type(self, t):
        t = t.upper()

        if t == "DATE":
            return Date()
        if t.startswith("DATETIME"):
            return DateTime()
        if t == "SMALLINT":
            return SmallInteger()
        if t == "INT":
            return Integer()
        if t == "BIGINT":
            return BigInteger()
        if t.startswith("DECIMAL"):
            inside = t[t.find("(") + 1 : t.find(")")]
            p, s = inside.split(",")
            return Numeric(int(p), int(s))
        if t.startswith("NVARCHAR(MAX)"):
            return String(None)
        if t.startswith("NVARCHAR("):
            n = t[t.find("(") + 1 : t.find(")")]
            return String(int(n))

        return String(None)

    def create_tables(self):
        if self.engine is None:
            display(Markdown("### ❌ Engine is not initialized. Call build_engine() first."))
            return

        created_tables = {}

        for table_name, table_def in self.cfg["tables"].items():
            display(Markdown(f"---\n## ▶️ Creating table: **{table_name}**"))

            schema = table_def.get("schema", "")
            metadata = MetaData(schema=schema if schema else None)

            cols = []
            for field in table_def["fields"]:
                col_name = field["db"]
                col_type = field["type"]

                if not col_name:
                    display(Markdown(f"⚠️ Skipped field with empty db name in `{table_name}`"))
                    continue

                try:
                    col_obj = Column(col_name, self.map_type(col_type))
                    cols.append(col_obj)
                except Exception as e:
                    display(Markdown(f"### ❌ Error mapping type for `{col_name}`:\n```\n{str(e)}\n```"))

            table_obj = Table(table_name, metadata, *cols)

            try:
                metadata.create_all(self.engine)
                display(Markdown(f"### ✅ Table `{table_name}` created successfully."))
            except SQLAlchemyError as e:
                display(Markdown(f"### ❌ Error creating table `{table_name}`:\n```\n{str(e)}\n```"))
                continue

            result_info = []
            for c in cols:
                result_info.append({
                    "column": c.name,
                    "type": str(c.type)
                })

            created_tables[table_name] = result_info

            display(Markdown("### ✔ Finished.\n"))

        display(Markdown("# 🎉 All table creation tasks completed."))
        return created_tables





db = DatabaseBuilder()
db.build_engine()        # test + create engine

result = db.create_tables()   # create all tables
result
