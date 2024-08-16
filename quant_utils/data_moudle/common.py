import datetime
from typing import Union

import pandas as pd
from dateutil.parser import parse

from quant_utils.db_conn import (
    DB_CONN_DATAYES,
    DB_CONN_JJTG_DATA,
    DB_CONN_JY,
    DB_CONN_JY_LOCAL,
    DB_CONN_WIND,
)
from quant_utils.data_moudle import utils


def get_index_close(
    ticker_symbol: Union[str, list[str]] = None,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    获取指数收盘价

    Parameters
    ----------
    ticker_symbol : Union[str, list[str]], optional
        指数代码或代码list, by default None
    start_date : str, optional
        开始时间, by default None
    end_date : str, optional
        结束时间, by default None

    Returns
    -------
    pd.DataFrame
        columns = [TICKER_SYMBOL, TRADE_DT, S_DQ_CLOSE]
    """
    # 如果结束时间为None,则结束时间取今日
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    sql_query = f"""
    SELECT
        SecuCode as TICKER_SYMBOL,
        DATE_FORMAT( TradingDay, '%Y%m%d' ) AS TRADE_DT,
        ClosePrice as S_DQ_CLOSE
    FROM
        jy_indexquote
    WHERE
        TradingDay BETWEEN "{start_date}"
        AND "{end_date}"
    """

    if ticker_symbol:
        code_str = utils.convert_list_to_str(ticker_symbol)
        sql_query += f" and SecuCode in ({code_str})"
    sql_query += " ORDER BY TRADE_DT, TICKER_SYMBOL"

    return DB_CONN_JJTG_DATA.exec_query(sql_query)


def get_style_index(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    获取国证风格指数收盘价

    Parameters
    ----------
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    pd.DataFrame
        风格指数收盘价
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    # 指数代码及对应风格
    index_codes = {
        "CBA00301": "cash",
        "399372": "large_growth",
        "399373": "large_value",
        "399374": "mid_growth",
        "399375": "mid_value",
        "399376": "small_growth",
        "399377": "small_value",
    }

    query_sql = f"""
    SELECT
        TradingDay AS TRADE_DT,
        SecuCode AS TICKER_SYMBOL,
        ClosePrice AS S_DQ_CLOSE 
    FROM
        jy_indexquote 
    WHERE
        1 = 1 
        AND SecuCode IN ( "399372", "399373", "399374", "399375", "399376", "399377" ) 
        AND TradingDay between '{start_date}' and "{end_date}" UNION
        SELECT
            a.TRADE_DT,
            a.TICKER_SYMBOL,
            a.S_DQ_CLOSE 
        FROM
            bond_chinabondindexquote a
            JOIN md_tradingdaynew b ON b.TRADE_DT = a.trade_dt 
        WHERE
            b.SECU_MARKET = 83
            AND b.IF_TRADING_DAY = 1 
            AND a.ticker_symbol = 'CBA00301' 
            AND a.TRADE_DT between '{start_date}' and "{end_date}"
    """
    # 获取指数收盘价格
    index_result = DB_CONN_JJTG_DATA.exec_query(query_sql)
    index_result["TRADE_DT"] = index_result["TRADE_DT"].apply(
        lambda s: s.strftime("%Y%m%d")
    )
    # # 进行数据透视，格式转换
    index_result = index_result.pivot_table(
        index="TRADE_DT", columns="TICKER_SYMBOL", values="S_DQ_CLOSE"
    )
    # 将代码列名修改为风格名
    index_result.rename(columns=index_codes, inplace=True)
    # 返回结果
    return index_result.dropna()


def get_dy1d_factor_ret_cne6_sw21(
    start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    获取CNE6中21年申万一级行业数据

    Parameters
    ----------
    start_date : str
        开始时间
    end_date : str
        结束时间

    Returns
    -------
    pd.DataFrame
        columns: 20个风格因子 + 31个行业因子 + 1个国家因子
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    sql_query = f"""
        SELECT
            *
        FROM
            dy1d_factor_ret_cne6_sw21
        WHERE
            TRADE_DATE between '{start_date}' and '{end_date}'
            ORDER BY TRADE_DATE
    """

    factor_ret = DB_CONN_DATAYES.exec_query(sql_query)
    factor_ret.drop(columns=["ID", "UPDATE_TIME"], inplace=True)
    factor_ret.rename(columns={"TRADE_DATE": "TRADE_DT"}, inplace=True)
    factor_ret = factor_ret.set_index("TRADE_DT")

    for col in factor_ret.columns:
        factor_ret[col] = factor_ret[col].astype("float")

    return factor_ret.dropna(axis=1)


def get_adjust_factor(ticker_symbol: str) -> pd.DataFrame:

    query_sql = f"""
        SELECT
            DATE_FORMAT(A.TradingDate,"%Y-%m-%d") as date,
            d.SecuCode,
            B.RatioAdjustingFactor 
        FROM
            qt_tradingdaynew A
            JOIN qt_adjustingfactor B
            JOIN secumainall d ON d.InnerCode = B.InnerCode 
        WHERE
            1 = 1 
            AND a.SecuMarket = 83 
            AND d.SecuCode = '{ticker_symbol}'
            AND A.TradingDate <= CURDATE() 
            AND B.ExDiviDate =(
            SELECT
                ExDiviDate 
            FROM
                qt_adjustingfactor 
            WHERE
                InnerCode = d.InnerCode 
                AND ExDiviDate <= A.TradingDate 
            ORDER BY
                ExDiviDate DESC
                LIMIT 1 
            ) 
        ORDER BY
            TradingDate
    """
    return DB_CONN_JY_LOCAL.exec_query(query_sql)


__all__ = [
    "get_index_close",
    "get_style_index",
    "get_dy1d_factor_ret_cne6_sw21",
    "get_adjust_factor",
]

if __name__ == "__main__":
    print(
        [
            name
            for name in globals()
            if not name.startswith("_")
            and (callable(globals()[name]) or isinstance(globals()[name], type))
        ]
    )
