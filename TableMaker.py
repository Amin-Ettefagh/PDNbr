import os
import importlib.util
import pyodbc


class DatabaseTableBuilder:

    TABLE_COLLATION = "SQL_Latin1_General_CP1256_CI_AS"

    def __init__(self):
        self.config = self.load_config()
        self.conn = self.connect()

    # ---------------- Load Config.py ----------------
    def load_config(self):
        config_path = os.path.join(os.getcwd(), "Config.py")

        if not os.path.isfile(config_path):
            raise RuntimeError("Config.py not found in current directory")

        spec = importlib.util.spec_from_file_location("Config", config_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "CONFIG"):
            raise RuntimeError("CONFIG not found inside Config.py")

        return module.CONFIG

    # ---------------- Connect ----------------
    def connect(self):
        cfg = self.config

        if cfg["database_type"].lower() != "sqlserver":
            raise NotImplementedError("Only SqlServer supported")

        if cfg["auth_method"].lower() == "winauth":
            conn_str = (
                f"DRIVER={{{cfg['pyodbc_driver']}}};"
                f"SERVER={cfg['server_ip']};"
                f"DATABASE={cfg['database_name']};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{cfg['pyodbc_driver']}}};"
                f"SERVER={cfg['server_ip']};"
                f"DATABASE={cfg['database_name']};"
                f"UID={cfg['username']};"
                f"PWD={cfg['password']};"
            )

        return pyodbc.connect(conn_str, autocommit=True)

    # ---------------- Table Exists ----------------
    def table_exists(self, cursor, table_name):
        cursor.execute("""
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = ?
        """, table_name)
        return cursor.fetchone() is not None

    # ---------------- Build CREATE TABLE ----------------
    def build_create_sql(self, table_name, table_def):
        cols = []
    
        for f in table_def["fields"]:
            col_type = f["type"].upper()
    
            if col_type.startswith("NVARCHAR"):
                col = (
                    f"[{f['name']}] {f['type']} "
                    f"COLLATE {self.TABLE_COLLATION} "
                    f"NULL"
                )
            else:
                col = (
                    f"[{f['name']}] {f['type']} NULL"
                )
    
            cols.append(col)
    
        columns_sql = ",\n  ".join(cols)
    
        return f"""
    CREATE TABLE [{table_name}]
    (
      {columns_sql}
    )
    """

    # ---------------- Run ----------------
    def run(self):
        cursor = self.conn.cursor()

        for table_name, table_def in self.config["tables"].items():
            if self.table_exists(cursor, table_name):
                print(f"TABLE EXISTS: {table_name}")
                continue

            sql = self.build_create_sql(table_name, table_def)
            cursor.execute(sql)
            print(f"TABLE CREATED: {table_name}")

        cursor.close()
        self.conn.close()
