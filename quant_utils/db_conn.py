from quant_utils.constant import DB_CONFIG
from quant_utils.database import MySQL

# 本地mysql连接
local_mysql = {
    "host": "192.20.57.188",
    "user": "chen",
    "pwd": "@1991727Asdf",
    "database": "1min",
    "port": 3306,
}


def create_conn_str(config: dict) -> str:
    return f"mysql+pymysql://{config['user']}:{config['pwd']}@{config['host']}:{config['port']}/{config['database']}?charset=utf8"


def crate_database_uri(database_type: str, config: dict) -> str:
    return f"{database_type}://{config['user']}:{config['pwd']}@{config['host']}:{config['port']}/{config['database']}"


JJTG_URI = crate_database_uri("mysql", DB_CONFIG["jjtg"])
JY_URI = crate_database_uri("mysql", DB_CONFIG["jy"])
JY_LOCAL_URI = crate_database_uri("mysql", DB_CONFIG["jy_local"])

DB_CONN_JY = MySQL(**DB_CONFIG["jy"])
DB_CONN_DATAYES = MySQL(**DB_CONFIG["datayes"])
DB_CONN_WIND = MySQL(**DB_CONFIG["wind"])
DB_CONN_LOCAL_MYSQL = MySQL(**local_mysql)
DB_CONN_WIND_TEST = MySQL(**DB_CONFIG["wind"])
DB_CONN_JY_TEST = MySQL(**DB_CONFIG["jy"])
DB_CONN_JJTG_DATA = MySQL(**DB_CONFIG["jjtg"])
DB_CONN_JY_LOCAL = MySQL(**DB_CONFIG["jy_local"])
