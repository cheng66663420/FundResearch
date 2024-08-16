from sqlalchemy import create_engine

from quant_utils.constant import DB_CONFIG
from quant_utils.utils import load_json

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "15rem",
    "padding": "2rem 1rem",
    "background-color": "rgb(242,242,242)",  # "rgb(231,227,218)",
}

CONTENT_STYLE = {
    "margin-left": "17rem",
    "margin-right": "2rem",
    "padding": "2rem" "1rem",
    "background-image": 'url("/assets/水印.png")',
    "background-repeat": "repeat",
    "background-position": "center",
    "background-size": "auto",
    "min-height": "100vh",  # 确保div至少有视口的高度
    "position": "relative",  # 设置相对定位
}

HEADER_STYLE = {
    "color": "#921235",
    "font-size": "30px",
    "font-weight": "bold",
}

CHART_THEME = "plotly_white"

DATE_FORMAT = "%Y-%m-%d"

DATABASE_URI = f'mysql+pymysql://{DB_CONFIG["jjtg"]["user"]}:{DB_CONFIG["jjtg"]["pwd"]}@{DB_CONFIG["jjtg"]["host"]}/{DB_CONFIG["jjtg"]["database"]}'

JJTG_ENGINE = create_engine(
    DATABASE_URI,
    max_overflow=10,
    pool_recycle=3600,
    pool_size=10,
)
APP_CONFIG = load_json("d:/config/app_config.json")

__all__ = [
    "SIDEBAR_STYLE",
    "CONTENT_STYLE",
    "CHART_THEME",
    "DATE_FORMAT",
    "JJTG_ENGINE",
    "HEADER_STYLE",
    "APP_CONFIG",
]
