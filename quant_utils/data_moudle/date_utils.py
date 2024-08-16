import datetime
from typing import Union

import numpy as np
import pandas as pd
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

from quant_utils.data_moudle import utils
from quant_utils.db_conn import (
    DB_CONN_JJTG_DATA,
    DB_CONN_JY,
    DB_CONN_JY_LOCAL,
    DB_CONN_WIND,
)


def get_period_end_date(
    start_date: str = None,
    end_date: str = None,
    period: str = "w",
    if_trading_day: int = 1,
) -> list[str]:
    """
    获取指定周期的结束日期

    Parameters
    ----------
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None
    period : str, optional
        周期, by default "w"
    if_trading_day : int, optional
        是否交易日, by default 1

    Returns
    -------
    list[str]
        指定周期的日期列表
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    # 对应的周期字典

    period_dict = {
        "d": "TRADE_DT",
        "w": "WEEK",
        "m": "MONTH",
        "q": "quarter",
        "y": "YEAR",
    }

    # 判断是否是字符串或者是否在字典的key里
    if not isinstance(period, str) or period.lower() not in period_dict:
        raise ValueError("周期不合法")

    trade_sql = "AND if_trading_day=1"
    if if_trading_day != 1:
        trade_sql = ""
    temp = period_dict[period] + "_END_DATE" if period != "d" else period_dict[period]
    query_sql = f"""
    SELECT DISTINCT
        DATE_FORMAT({temp}, '%Y%m%d') as 'END_DATE'
    FROM
        md_tradingdaynew
    WHERE
        {temp} between '{start_date}' and '{end_date}'
        AND secu_market = 83
        {trade_sql}
    ORDER BY
        END_DATE
    """

    return DB_CONN_JJTG_DATA.exec_query(query_sql)["END_DATE"].tolist()


def get_recent_period_end_date_dict(
    end_date: str, start_date: str = None, dates_dict: dict = None, if_cn=1
) -> dict:
    """
    根据end_date获取日期区间,
    如果start_date不为None,
    区间时间段均需要大于start_date

    Parameters
    ----------
    end_date : str
        结束日期
    start_date : str, optional
        起始日期, by default None
    dates_dict: dict
        需要计算的日期字典
    Returns
    -------
    dict
        日期字典, 格式为{dict[日期名称]: (区间开始日期, 区间结束日期)}
    """
    if dates_dict is None:
        dates_dict = {
            "d": [1],
            "w": [1],
            "m": [
                1,
                2,
                3,
                6,
                9,
            ],
            "y": [1, 2, 3, 4, 5],
        }
    result_date_dict = {}
    date_name_dict = {"d": "日", "w": "周", "m": "月", "y": "年"}

    # 需要计算的日期
    for key, val in dates_dict.items():
        for num in val:
            temp_date = offset_period_trade_dt(trade_date=end_date, n=-num, period=key)

            if start_date is None or temp_date >= start_date:
                if if_cn:
                    result_date_dict[f"近{num}{date_name_dict[key]}"] = (
                        temp_date,
                        end_date,
                    )
                else:
                    result_date_dict[f"{num}{(key.upper())}"] = (temp_date, end_date)
    return result_date_dict


def get_recent_trade_dt(trade_date: str) -> str:
    """
    获取最近一个交易日，不含当天

    Parameters
    ----------
    trade_date : str
        交易日

    Returns
    -------
    str
        最近一个交易日，不含当天
    """
    query_sql = f"""
    SELECT
        DATE_FORMAT(max( TRADE_DT ), '%Y%m%d') as TRADE_DT
    FROM
        md_tradingdaynew 
    WHERE
        1 = 1 
        AND SECU_MARKET = 83 
        AND IF_TRADING_DAY = 1 
        AND TRADE_DT < '{trade_date}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["TRADE_DT"].tolist()[-1]


def offset_trade_dt(
    trade_date: str, n_days_before: int, if_include_today: bool = 0
) -> str:
    """
    获取n天前的交易日

    Parameters
    ----------
    trade_date : str
        基点交易日.
    n_days_before : int
        需要前移的交易.
    if_include_today: bool
        是否包括今日,默认为否
    Returns
    -------
    str
        n天前的交易日.

    """
    sign = np.sign(n_days_before)
    n_days_before = abs(n_days_before)
    if sign == -1:
        order = "ASC"
        fuhao = ">="
    else:
        order = "DESC"
        fuhao = "<="

    ndays = n_days_before - 1 if if_include_today else n_days_before
    query_sql = f"""
        SELECT
            DATE_FORMAT(TRADE_DT , '%Y%m%d') as TRADE_DT
        FROM
            md_tradingdaynew
        WHERE
            IF_TRADING_DAY = 1
            AND TRADE_DT {fuhao} '{trade_date}'
            AND SECU_MARKET = 83
        ORDER BY
            TRADE_DT {order}
            LIMIT {ndays}, 1
    """

    return DB_CONN_JJTG_DATA.exec_query(query_sql)["TRADE_DT"].tolist()[-1]


def offset_period_dt(trade_date: str, n: int, period: str = "d") -> str:
    """
    日期向前推n个周期(自然日),offset表示方向,-1为向前,1为向后

    Parameters
    ----------
    trade_date : str
        当前日期.
    n : int
        n个周期, 负数为向前，正数为向后
    period : str
        周期， d日, w周, m月, ,y年.
    Returns
    -------
    str
        DESCRIPTION.
    """

    datetime = parse(trade_date)
    # 如果日,则返回改日前n个交易日
    if period == "d":
        temp_date = datetime + relativedelta(days=n)

    elif period == "m":
        temp_date = datetime + relativedelta(months=n)

    elif period == "w":
        temp_date = datetime + relativedelta(weeks=n)

    elif period == "y":
        temp_date = datetime + relativedelta(years=n)

    return temp_date.strftime("%Y%m%d")


def offset_period_trade_dt(trade_date: str, n: int, period: str = "d") -> str:
    """
    日期向前推n个周期(交易日),offset表示方向,-1为向前,1为向后

    Parameters
    ----------
    trade_date : str
        当前日期.
    n : int
        n个周期, 负数为向前，正数为向后
    period : str
        周期， d日, w周, m月, ,y年.
    Returns
    -------
    str
        n个周期的交易日.
    """
    # n = -n
    if period == "d":
        n = -n
        return offset_trade_dt(trade_date, n)
    return offset_trade_dt(offset_period_dt(trade_date, n, period), n_days_before=0)


def if_trade_dt(trade_date: str) -> bool:
    """
    判断某日期是否为交易日

    Parameters
    ----------
    trade_date : str
        交易日字符串

    Returns
    -------
    bool
        是否交易日，1为是，0为否
    """

    sql_query = f"""
    SELECT
		IF_TRADING_DAY
	FROM
		md_tradingdaynew
	WHERE
		TRADE_DT = '{trade_date}'
		AND SECU_MARKET = 83
    """
    return DB_CONN_JJTG_DATA.exec_query(sql_query).loc[0, "IF_TRADING_DAY"]


def get_last_peroid_end_date(end_date: str, period: str = "q") -> str:
    """
    根据日期获的前一个月末、周末、季末、年末交易日

    Parameters
    ----------
    end_date : str
        需要计算的日期
    period : str, optional
        需要计算的, by default "q"

    Returns
    -------
    str
        日期字符串
    """
    period_dict = {
        "w": "WEEK",
        "m": "MONTH",
        "q": "quarter",
        "y": "YEAR",
    }
    if period not in period_dict:
        raise ValueError(f"{period}区间不在函数计算内")

    query_sql = f"""
    SELECT
        date_format(max( TRADE_DT ) , "%Y%m%d") as END_DATE
    FROM
        md_tradingdaynew 
    WHERE
        1 = 1 
        AND SECU_MARKET = '83' 
        AND TRADE_DT < '{end_date}' 
        AND IF_{period_dict[period]}_END = 1
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["END_DATE"].values[0]


def if_period_end(trade_dt: str, period="WEEK") -> int:
    """
    判断是否为区间结束日

    Parameters
    ----------
    trade_dt : str
        日期
    period : str, optional
        区间范围，WEEK, MONTH, QUARTER, YEAR, by default "WEEK"

    Returns
    -------
    int
        1代表是，0代表否
    """
    query_sql = f"""
    SELECT
        IF_{period}_END
    FROM
        md_tradingdaynew 
    WHERE
        1 = 1 
        AND SECU_MARKET = 83
        and TRADE_DT = '{trade_dt}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)[f"IF_{period}_END"].values[0]


def get_period_end(start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取区间结束日，并标注名称

    Parameters
    ----------
    start_date : str
        区间开始日期
    end_date : str
        区间结束日期

    Returns
    -------
    pd.DataFrame
        columns = [PERIOD, END_DATE, START_DATE, PERIOD_NAME]
    """
    query_sql = f"""
    WITH a AS ( 
        SELECT max( TRADE_DT ) AS TRADE_DT 
        FROM md_tradingdaynew 
        WHERE 1 = 1 AND trade_dt <= '{end_date}' AND `SECU_MARKET` = '83' 
    ),
    b AS (
        SELECT
            b.TRADE_DT,
            b.MONTH_END_DATE,
            b.QUARTER_END_DATE,
            b.YEAR_END_DATE 
        FROM
            md_tradingdaynew b
            JOIN a ON a.trade_dt = b.trade_dt 
        WHERE
            1 = 1 
            AND b.`SECU_MARKET` = '83' 
        ) 
    SELECT
        CONCAT( YEAR ( TRADE_DT ), '-', LPAD( MONTH ( TRADE_DT ), 2, 0 ) ) AS PERIOD,
        DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
        DATE_FORMAT( MONTH_END_DATE, "%Y%m%d" ) AS START_DATE,
        "月度" as PERIOD_NAME
    FROM
        md_tradingdaynew 
    WHERE
        1 = 1 
        AND `SECU_MARKET` = '83' 
        AND IF_MONTH_END = 1 
        and TRADE_DT between "{start_date}" and "{end_date}" UNION
    SELECT
        CONCAT(YEAR ( TRADE_DT ),'-','Q', round( MONTH ( TRADE_DT )/ 3 )) AS PERIOD,
        DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
        DATE_FORMAT( QUARTER_END_DATE, "%Y%m%d" ) AS START_DATE,
        "季度" as PERIOD_NAME
    FROM
        md_tradingdaynew 
    WHERE
        1 = 1 
        AND `SECU_MARKET` = '83' 
        AND IF_QUARTER_END = 1 
        and TRADE_DT between "{start_date}" and "{end_date}" UNION
    SELECT
        CONCAT( YEAR ( TRADE_DT ),'年') AS PERIOD, 
        DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
        DATE_FORMAT( YEAR_END_DATE, "%Y%m%d" ) AS START_DATE,
        "年度" as PERIOD_NAME
    FROM
        md_tradingdaynew 
    WHERE
        1 = 1 
        AND `SECU_MARKET` = '83' 
        AND IF_YEAR_END = 1
        and TRADE_DT between "{start_date}" and "{end_date}" 
    union SELECT
        CONCAT(
            YEAR ( TRADE_DT ),
            '-',
            'Q',
        round( MONTH ( TRADE_DT )/ 3 )) AS PERIOD,
        DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
        DATE_FORMAT( QUARTER_END_DATE, "%Y%m%d" ) AS START_DATE,
        "季度" AS PERIOD_NAME 
    FROM
        b UNION
    SELECT
        CONCAT( YEAR ( TRADE_DT ), '-', LPAD( MONTH ( TRADE_DT ), 2, 0 ) ) AS PERIOD,
        DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
        DATE_FORMAT( MONTH_END_DATE, "%Y%m%d" ) AS START_DATE,
        "月度" AS PERIOD_NAME 
    FROM
        b UNION
    SELECT
        CONCAT( YEAR ( TRADE_DT ), '年' ) AS PERIOD,
        DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
        DATE_FORMAT( YEAR_END_DATE, "%Y%m%d" ) AS START_DATE,
        "年度" AS PERIOD_NAME 
    FROM
        b
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql).drop_duplicates()


def get_trade_cal(start_date: str, end_date: str) -> list:
    """
    获取交易

    Parameters
    ----------
    start_date : str
        开始日期
    end_date : _type_
        结束日期

    Returns
    -------
    list
        交易日期列表
    """
    query_sql = f"""
    SELECT
	DATE_FORMAT( A.TradingDate, "%Y%m%d" ) AS trade_time
    FROM
	qt_tradingdaynew A
    where 
    1=1
    and TradingDate between '{start_date}' and '{end_date}'
    and SecuMarket = 83 
    and IfTradingDay = 1
    order by trade_time
    """
    return DB_CONN_JY_LOCAL.exec_query(query_sql)["trade_time"].tolist()


__all__ = [
    "get_period_end_date",
    "get_recent_period_end_date_dict",
    "get_recent_trade_dt",
    "offset_trade_dt",
    "offset_period_dt",
    "offset_period_trade_dt",
    "if_trade_dt",
    "get_last_peroid_end_date",
    "if_period_end",
    "get_period_end",
    "get_trade_cal",
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
