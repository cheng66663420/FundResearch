# -*- coding: utf-8 -*-

import pandas as pd

from quant_utils.db_conn import DB_CONN_JJTG_DATA

DB_CONN = DB_CONN_JJTG_DATA


def get_portfolio_indicator_score(trade_dt: str) -> pd.DataFrame:
    """
    获取组合指标得分

    Parameters
    ----------
    trade_dt : str
        日期

    Returns
    -------
    pd.DataFrame
        结果的df
    """
    query_sql = f"""
    SELECT
        *
	FROM
        view_portfolio_evaluation_indicator_score
	WHERE
        1 = 1 
        AND END_DATE = '{trade_dt}'
        AND WEIGHT != 0
    """
    return DB_CONN.exec_query(query_sql)


def get_portfolio_score(trade_dt: str) -> pd.DataFrame:
    """
    获取组合指标打分细项

    Parameters
    ----------
    trade_dt : str
        日期

    Returns
    -------
    pd.DataFrame
        每个组合子指标得分
    """
    query_sql = f"""
    SELECT
        *
	FROM
        view_portfolio_evaluation_sub_score
	WHERE
        1 = 1 
        AND END_DATE = '{trade_dt}'
        AND WEIGHT != 0
    """
    return DB_CONN.exec_query(query_sql)


def get_portfolio_evalution(trade_dt: str) -> pd.DataFrame:
    """
    获取组合评价结果

    Parameters
    ----------
    trade_dt : str
        日期

    Returns
    -------
    pd.DataFrame
        每个组合最终的结果
    """
    query_sql = f"""
    SELECT
        *
	FROM
        view_portfolio_evaluation
	WHERE
        1 = 1 
        AND END_DATE = '{trade_dt}'
    """
    return DB_CONN.exec_query(query_sql)


def portfolio_evlalution_main(trade_dt):
    df1 = get_portfolio_indicator_score(trade_dt=trade_dt)
    df2 = get_portfolio_score(trade_dt=trade_dt)
    df3 = get_portfolio_evalution(trade_dt=trade_dt)
    with pd.ExcelWriter(f"D:/底稿/{trade_dt}.xlsx") as writer:
        df3.to_excel(writer, sheet_name="组合总分", index=False)
        df2.to_excel(writer, sheet_name="组合子项得分", index=False)
        df1.to_excel(writer, sheet_name="组合指标得分", index=False)


if __name__ == "__main__":
    trade_dt = "20230727"
    portfolio_evlalution_main(trade_dt)
