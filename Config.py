# -----------------------------------------------------------
# Application Configuration File (Expandable / Maintainable)
# -----------------------------------------------------------

CONFIG = {
    # ---------------------------------------
    # Server Settings
    # ---------------------------------------

    # Database server IP address
    "server_ip": "1.1.1.1",

    # Database type
    # Options (expandable):
    #   - "SqlServer"
    #   - "MySQL"
    #   - "Oracle"
    #   - "Postgres"
    #   - ...
    "database_type": "SqlServer",

    # ---------------------------------------
    # Authentication Settings
    # ---------------------------------------

    # Username for database connection
    "username": "a.etefagh",

    # Password for database connection
    "password": "123456789",

    # Authentication method
    # Options:
    #   - "WinAuth"  -> Windows Authentication
    #   - "UserPass" -> Username/Password Authentication
    "auth_method": "WinAuth",

    # ---------------------------------------
    # Operating System Settings
    # ---------------------------------------

    # Operating system type
    # Options:
    #   - "Windows"
    #   - "Linux"
    "os_type": "Windows",

    # ---------------------------------------
    # Database Settings
    # ---------------------------------------

    # Database name
    "database_name": "AminTest",
}
