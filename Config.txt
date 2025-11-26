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
    # Each table has: schema, fields, input file, sheet name (optional)
    # ---------------------------------------

    "tables": {

        "Users": {
            "schema": "dbo",

            # Input File Information
            "input": {
                "file": r"D:\Data\Users.xlsx",   # Excel or CSV
                "sheet": "Sheet1"                # If None or "", load the first sheet
            },

            "fields": {
                "id": {
                    "excel": "UserID_Excel",
                    "db": "UserID",
                    "type": "INT"
                },
                "username": {
                    "excel": "UserName_Excel",
                    "db": "UserName",
                    "type": "NVARCHAR(100)"
                },
                "password": {
                    "excel": "Password_Excel",
                    "db": "PasswordHash",
                    "type": "NVARCHAR(200)"
                },
                "email": {
                    "excel": "Email_Excel",
                    "db": "Email",
                    "type": "NVARCHAR(150)"
                },
                "created_at": {
                    "excel": "CreatedAt_Excel",
                    "db": "CreatedAt",
                    "type": "DATETIME"
                }
            }
        },

        "Products": {
            "schema": "inventory",

            "input": {
                "file": r"D:\Data\Products.csv",
                "sheet": None         # None → Auto-load first sheet OR ignore for CSV
            },

            "fields": {
                "id": {
                    "excel": "ProductID_Excel",
                    "db": "ProductID",
                    "type": "INT"
                },
                "name": {
                    "excel": "ProductName_Excel",
                    "db": "ProductName",
                    "type": "NVARCHAR(150)"
                },
                "price": {
                    "excel": "Price_Excel",
                    "db": "Price",
                    "type": "DECIMAL(18,2)"
                },
                "stock": {
                    "excel": "Stock_Excel",
                    "db": "StockQty",
                    "type": "INT"
                },
                "created_at": {
                    "excel": "CreatedAt_Excel",
                    "db": "CreatedAt",
                    "type": "DATETIME"
                }
            }
        }
    }
}
