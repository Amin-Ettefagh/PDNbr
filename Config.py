# -----------------------------------------------------------
# Application Configuration File (Expandable / Maintainable)
# -----------------------------------------------------------

CONFIG = {
    # ---------------------------------------
    # Server Settings
    # ---------------------------------------

    "server_ip": "1.1.1.1",

    # Database type
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
    # Database Tables, Schemas & Fields
    # Support for multiple tables
    # ---------------------------------------

    "tables": {
        "Users": {
            "schema": "dbo",
            "fields": {
                "id": "UserID",
                "username": "UserName",
                "password": "PasswordHash",
                "email": "Email",
                "created_at": "CreatedAt"
            }
        },

        "Products": {
            "schema": "inventory",
            "fields": {
                "id": "ProductID",
                "name": "ProductName",
                "price": "Price",
                "stock": "StockQty",
                "created_at": "CreatedAt"
            }
        },

        "Logs": {
            "schema": "system",
            "fields": {
                "id": "LogID",
                "level": "Level",
                "message": "Message",
                "created_at": "CreatedAt"
            }
        }
    }
}
