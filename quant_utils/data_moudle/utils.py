import datetime
from typing import Union

import pandas as pd
from dateutil.parser import parse

from quant_utils.db_conn import (
    DB_CONN_JJTG_DATA,
    DB_CONN_JY,
    DB_CONN_JY_LOCAL,
    DB_CONN_WIND,
)


def get_report_date(date: str, num_period: int) -> list:
    """
    寻找日期前的n个报告期,不含当前季末

    Parameters
    ----------
    date : str
        日期.
    num_period : int
        需要寻找的n个报告期.

    Returns
    -------
    list
        报告期list.
    """

    # 将字符串解析为datetime
    date_time = parse(date)
    # 结果list
    date_list = []
    # 往前推num期
    for _ in range(num_period):
        temp_date = None
        # 时间减3个月
        temp_month = date_time.month - 3
        # 寻找对应的季末
        if temp_month > 9:
            temp_date = date_time.replace(month=12, day=31)

        elif temp_month > 6:
            temp_date = date_time.replace(month=9, day=30)

        elif temp_month > 3:
            temp_date = date_time.replace(month=6, day=30)

        elif temp_month > 0:
            temp_date = date_time.replace(month=3, day=31)

        else:
            temp_date = date_time.replace(month=12, day=31, year=date_time.year - 1)

        date_time = temp_date
        date_list.append(temp_date.strftime("%Y%m%d"))
    # 逆序
    date_list.reverse()
    return date_list


def get_now():
    return datetime.datetime.now().strftime("%Y%m%d")


def convert_list_to_str(params: Union[str, list[str]]) -> str:
    """
    如果是list就将list转化为str,并加引号

    Parameters
    ----------
    params : str or list[str]
        需要转换的字符串或者list

    Returns
    -------
    str
        转化后的结果
    """
    if isinstance(params, list):
        params_list = [f"'{param}'" for param in params]
        result = ",".join(params_list)
    elif isinstance(params, str):
        result = f"'{params}'"
    else:
        raise ValueError("输入不合法")

    return result


def prepare_dates(start_date: str = None, end_date: str = None) -> tuple[str]:
    """
    准备开始时间与结束时间

    Parameters
    ----------
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    tuple[str]
        _description_
    """
    if start_date is None:
        start_date = "19900101"
    if end_date is None:
        end_date = "20991231"

    return (start_date, end_date)


def fill_sql_ticker_symbol(
    ticker_symbol: Union[str, list[str]], sql_query: str, tikcer_symbol_name: str
) -> str:
    """
    将ticker_symbol填充入sql语句, ticker_symbol可以是str也可以是list

    Parameters
    ----------
    ticker_symbol : str or list[str]
        基金
    sql_query : str
        原始查询语句
    tikcer_symbol_name : str
        TICKER_SYMBOL在表中的名字
    Returns
    -------
    str
        填充ticker_symbol之后的sql语句
    """
    if ticker_symbol:
        ticker_symbol = convert_list_to_str(ticker_symbol)
        sql_query += f"and  {tikcer_symbol_name} in ({ticker_symbol})"

    return sql_query


def get_sector_industry_map() -> pd.DataFrame:
    """
    获取申万一级行业和板块对照表

    Returns
    -------
    pd.DataFrame
        申万一级行业和板块对照表
    """
    query_sql = """
    SELECT
        SECTOR,
        INDUSTRY_NAME
    FROM
        sector_industry
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


__all__ = [
    "get_report_date",
    "get_now",
    "convert_list_to_str",
    "prepare_dates",
    "fill_sql_ticker_symbol",
    "get_sector_industry_map",
]
if __name__ == "__main__":
    print(
        [
            name
            for name in globals()
            if not name.startswith("_")
            and callable(globals()[name])
            or isinstance(globals()[name], type)
        ]
    )
