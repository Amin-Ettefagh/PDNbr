# -----------------------------------------------------------
# Application Configuration File (Expandable / Maintainable)
# ETL Ready - Multi Table, Multi File, Multi Schema
# -----------------------------------------------------------

CONFIG = {
    # ---------------------------------------
    # Server Settings
    # ---------------------------------------

    "server_ip": "1.1.1.1",
    "database_type": "SqlServer",

    # ---------------------------------------
    # Authentication Settings
    # ---------------------------------------

    "username": "a.etefagh",
    "password": "123456789",
    "auth_method": "WinAuth",  # WinAuth or UserPass

    # ---------------------------------------
    # OS Settings
    # ---------------------------------------

    "os_type": "Windows",

    # ---------------------------------------
    # Database Settings
    # ---------------------------------------

    "database_name": "AminTest",
    "pyodbc_driver": "ODBC Driver 17 for SQL Server",

    # ---------------------------------------
    # Multi Table Definitions
    # Each table has: schema, fields (list), input file, sheet name (optional)
    # ---------------------------------------

    "tables": {
        "Users": {
            "schema": "dbo",

            # Input File Information
            "input": {
                "file": r"D:\Data\Users.xlsx",   # Excel or CSV
                "sheet": "Sheet1"                # If None or "", load the first sheet
            },

            # Each field is a dict: { excel, db, type }
            "fields": [
                {
                    "file": "UserID_Excel",
                    "db": "UserID",
                    "type": "INT"
                }
            ]
        },

        "Products": {
            "schema": "inventory",

            "input": {
                "file": r"D:\Data\Products.csv",
                "sheet": None   # None or "" -> auto-load first sheet (or ignored for CSV)
            },

            "fields": [
                {
                    "file": "UserID_Excel",
                    "db": "UserID",
                    "type": "INT"
                }
            ]
        }
    }
}
