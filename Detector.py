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
    def __init__(self, copy_file_to_db_name=False):
        self.cfg = CONFIG
        self.engine = None
        self.copy_file_to_db_name = copy_file_to_db_name

    def build_engine(self):
        driver = self.cfg["pyodbc_driver"]
        server = self.cfg["server_ip"]
        database = self.cfg["database_name"]
        username = self.cfg["username"]
        password = self.cfg["password"]
        auth = self.cfg["auth_method"]

        if auth == "WinAuth":
            conn = f"Driver={driver};Server={server};Database={database};Trusted_Connection=yes;"
        else:
            conn = f"Driver={driver};Server={server};Database={database};UID={username};PWD={password};"

        from urllib.parse import quote_plus
        conn_url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn)

        try:
            self.engine = create_engine(conn_url, fast_executemany=True)
            with self.engine.connect() as c:
                c.execute(text("SELECT 1"))
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
            inside = t[t.find("(")+1:t.find(")")]
            p, s = inside.split(",")
            return Numeric(int(p), int(s))
        if t.startswith("NVARCHAR(MAX)"):
            return String(None)
        if t.startswith("NVARCHAR("):
            n = int(t[t.find("(")+1:t.find(")")])
            return String(n)
        return String(None)

    def apply_collation_full(self, schema, table_name, collation, fields):
        fq = f"{schema}.{table_name}" if schema else table_name
        with self.engine.connect() as conn:
            try:
                conn.execute(text(f"ALTER TABLE {fq} COLLATE {collation}"))
            except:
                pass
            for f in fields:
                col_name = f["db"] if f["db"] else f["file"]
                col_type = f["type"].upper()
                if col_type.startswith("NVARCHAR"):
                    try:
                        conn.execute(text(
                            f"ALTER TABLE {fq} ALTER COLUMN {col_name} {col_type} COLLATE {collation}"
                        ))
                    except:
                        pass

    def create_tables(self):
        if self.engine is None:
            display(Markdown("### ❌ Engine not initialized"))
            return

        created_tables = {}

        for table_name, table_def in self.cfg["tables"].items():
            display(Markdown(f"---\n## ▶️ Creating table: **{table_name}**"))

            schema = table_def.get("schema", "")
            collation = table_def.get("collation", "SQL_Latin1_General_CP1256_CI_AS")

            metadata = MetaData(schema=schema if schema else None)

            cols = []
            for field in table_def["fields"]:
                db_name = field["db"]
                if self.copy_file_to_db_name and not db_name:
                    db_name = field["file"]
                if not db_name:
                    display(Markdown(f"⚠️ Field skipped (missing db name)"))
                    continue
                sqlalchemy_type = self.map_type(field["type"])
                if isinstance(sqlalchemy_type, String):
                    sqlalchemy_type = String(getattr(sqlalchemy_type, "length", None), collation=collation)
                col_obj = Column(db_name, sqlalchemy_type)
                cols.append(col_obj)

            table_obj = Table(table_name, metadata, *cols)

            try:
                metadata.create_all(self.engine)
                display(Markdown(f"### ✅ Table `{table_name}` created."))
            except SQLAlchemyError as e:
                display(Markdown(f"### ❌ Error creating `{table_name}`:\n```\n{str(e)}\n```"))
                continue

            self.apply_collation_full(schema, table_name, collation, table_def["fields"])
            display(Markdown(f"### 🔤 Collation `{collation}` applied."))

            result_info = [{"column": c.name, "type": str(c.type)} for c in cols]
            created_tables[table_name] = result_info

            display(Markdown("### ✔ Finished.\n"))

        display(Markdown("# 🎉 All tables created successfully"))
        return created_tables




#DatabaseBuilder(copy_file_to_db_name=True)

