from quant_utils.constant import DB_CONFIG
from quant_utils.database import MySQL
from urllib.parse import quote

# 本地mysql连接
local_mysql = {
    "db_type": "mysql",
    "host": "192.20.57.188",
    "user": "chen",
    "pwd": "@1991727Asdf",
    "database": "1min",
    "port": 3306,
}


def crate_database_uri(config: dict) -> str:
    return f"{config['db_type']}://{quote(config['user'])}:{quote(config['pwd'])}@{quote(config['host'])}:{config['port']}/{quote(config['database'])}"


JJTG_URI = crate_database_uri(DB_CONFIG["jjtg"])
JY_URI = crate_database_uri(DB_CONFIG["jy"])
JY_LOCAL_URI = crate_database_uri(DB_CONFIG["jy_local"])
PG_DATA_URI = crate_database_uri(DB_CONFIG["pg_data"])
DATAYES_URI = crate_database_uri(DB_CONFIG["datayes"])
WIND_URI = crate_database_uri(DB_CONFIG["wind"])

DB_CONN_JY = MySQL(JY_URI)
DB_CONN_DATAYES = MySQL(DATAYES_URI)
DB_CONN_WIND = MySQL(WIND_URI)
DB_CONN_LOCAL_MYSQL = MySQL(crate_database_uri(local_mysql))
DB_CONN_WIND_TEST = MySQL(WIND_URI)
DB_CONN_JY_TEST = MySQL(JY_URI)
DB_CONN_JJTG_DATA = MySQL(JJTG_URI)
DB_CONN_JY_LOCAL = MySQL(JY_LOCAL_URI)
