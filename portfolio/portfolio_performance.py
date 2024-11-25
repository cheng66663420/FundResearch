import datetime
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from tqdm import tqdm

import quant_utils.data_moudle as dm
from data_functions.portfolio_data import get_portfolio_info, query_portfolio_nav
from quant_utils.constant import DATE_FORMAT, TODAY
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.send_email import MailSender

RENAME_DICT = {
    "TICKER_SYMBOL": "组合名称",
    "CYCLE": "周期",
    "START_DATE": "起始日期",
    "END_DATE": "结束日期",
    "INDICATOR": "指标",
    "PORTFOLIO_VALUE": "组合",
    "PEER_RANK": "同类基金排名",
    "BENCHMARK_VALUE_OUTTER": "对客基准",
    "BENCHMARK_VALUE_INNER": "对内基准",
    "PEER_MEDIAN": "同类中位数",
    "PEER_FOF_RANK": "同类FOF排名",
    "PEER_PORTFOLIO_RANK": "同类投顾排名",
}


def get_portfolio_rank(portfolio_name: str, end_date: str) -> pd.DataFrame:
    peer_query = (
        get_portfolio_info()
        .query(f"PORTFOLIO_NAME == '{portfolio_name}'")["PEER_QUERY"]
        .values[0]
    )
    peer_query = peer_query.replace("LEVEL", "c.LEVEL")
    peer_query = peer_query.replace("==", "=")
    query_sql = f"""
        WITH a AS (
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            FROM
                portfolio_performance_inner a
                JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                AND a.END_DATE = b.END_DATE 
            WHERE
                1 = 1 
                AND a.END_DATE = '{end_date}' 
                AND ( a.TICKER_SYMBOL = b.PORTFOLIO_NAME OR b.PORTFOLIO_NAME = 'ALL' ) 
                AND a.TICKER_SYMBOL = '{portfolio_name}' UNION
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            FROM
                fund_performance_inner a
                JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                AND a.END_DATE = b.END_DATE
                JOIN fund_type_own c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL

            WHERE
                1 = 1 
                and {peer_query}
                AND a.END_DATE = '{end_date}' 
                AND ( b.PORTFOLIO_NAME = '{portfolio_name}' OR b.PORTFOLIO_NAME = 'ALL' ) 
            AND ( 
                c.REPORT_DATE = ( 
                    SELECT max( report_date ) 
                    FROM fund_type_own 
                    WHERE PUBLISH_DATE <= '{end_date}' 
                )
                )),
            b AS ( SELECT DATE_NAME, count(*) AS NUM FROM a GROUP BY DATE_NAME ) SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '累计收益率' AS INDICATOR,
            CUM_RETURN AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化收益率' AS INDICATOR,
            ANNUAL_RETURN AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化波动率' AS INDICATOR,
            ANNUAL_VOLATILITY AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '最大回撤' AS INDICATOR,
            MAXDD AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '收益波动比' AS INDICATOR,
            SHARP_RATIO_ANNUAL AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化收益回撤比' AS INDICATOR,
            CALMAR_RATIO_ANNUAL AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_portfolio_derivatives_rank(portfolio_name: str, end_date: str) -> pd.DataFrame:
    peer_query = (
        get_portfolio_info()
        .query(f"PORTFOLIO_NAME == '{portfolio_name}'")["PEER_QUERY"]
        .values[0]
    )
    peer_query = peer_query.replace("LEVEL", "c.LEVEL")
    peer_query = peer_query.replace("==", "=")
    query_sql = f"""
        WITH a AS (
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            FROM
                portfolio_derivatives_performance_inner a
                JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                AND a.END_DATE = b.END_DATE 
            WHERE
                1 = 1 
                AND a.END_DATE = '{end_date}' 
                AND ( a.TICKER_SYMBOL = b.PORTFOLIO_NAME OR b.PORTFOLIO_NAME = 'ALL' ) 
                AND a.TICKER_SYMBOL = '{portfolio_name}' UNION
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            FROM
                fund_performance_inner a
                JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                AND a.END_DATE = b.END_DATE
                JOIN fund_type_own c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL

            WHERE
                1 = 1 
                and {peer_query}
                AND a.END_DATE = '{end_date}' 
                AND ( b.PORTFOLIO_NAME = '{portfolio_name}' OR b.PORTFOLIO_NAME = 'ALL' ) 
            AND ( 
                c.REPORT_DATE = ( 
                    SELECT max( report_date ) 
                    FROM fund_type_own 
                    WHERE PUBLISH_DATE <= '{end_date}' 
                )
                )),
            b AS ( SELECT DATE_NAME, count(*) AS NUM FROM a GROUP BY DATE_NAME ) SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '累计收益率' AS INDICATOR,
            CUM_RETURN AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化收益率' AS INDICATOR,
            ANNUAL_RETURN AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化波动率' AS INDICATOR,
            ANNUAL_VOLATILITY AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '最大回撤' AS INDICATOR,
            MAXDD AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '收益波动比' AS INDICATOR,
            SHARP_RATIO_ANNUAL AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化收益回撤比' AS INDICATOR,
            CALMAR_RATIO_ANNUAL AS PORTFOLIO_VALUE,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC )* 100 AS PEER_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql).query(
        f"TICKER_SYMBOL == '{portfolio_name}'"
    )

    benchmark_df = get_benchmark_value_outter(
        portfolio_name=portfolio_name, end_date=end_date
    )
    benchmark_df.rename(
        columns={"BENCHMARK_VALUE_OUTTER": "BENCHMARK_VALUE_INNER"}, inplace=True
    )
    return df.merge(
        benchmark_df,
        how="left",
        on=["DATE_NAME", "TICKER_SYMBOL", "START_DATE", "END_DATE", "INDICATOR"],
    )


def get_fof_rank(portfolio_name: str, end_date: str) -> pd.DataFrame:
    query_sql = f"""
        WITH a AS (
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            FROM
                portfolio_performance_inner a
                JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                AND a.END_DATE = b.END_DATE 
            WHERE
                1 = 1 
                AND a.END_DATE = '{end_date}' 
                AND ( a.TICKER_SYMBOL = b.PORTFOLIO_NAME OR b.PORTFOLIO_NAME = 'ALL' ) 
                AND a.TICKER_SYMBOL = '{portfolio_name}' UNION
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            FROM
                fund_performance_inner a
                JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                AND a.END_DATE = b.END_DATE
                JOIN fof_type c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
            WHERE
                1 = 1 
                AND a.END_DATE = '{end_date}' 
                AND c.INNER_TYPE = '{portfolio_name}' 
                AND ( b.PORTFOLIO_NAME = '{portfolio_name}' OR b.PORTFOLIO_NAME = 'ALL' ) 
           ),
            b AS ( SELECT DATE_NAME, count(*) AS NUM FROM a GROUP BY DATE_NAME ) SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '累计收益率' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC ), '/', b.NUM ) AS PEER_FOF_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC )* 100 AS PEER_FOF_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化收益率' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC ), '/', b.NUM ) AS PEER_FOF_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC )* 100 AS PEER_FOF_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
             '年化波动率' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY ), '/', b.NUM ) AS PEER_FOF_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY )* 100 AS PEER_FOF_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '最大回撤' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD ), '/', b.NUM ) AS PEER_FOF_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD )* 100 AS PEER_FOF_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
              '收益波动比' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_FOF_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC )* 100 AS PEER_FOF_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
              '年化收益回撤比' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_FOF_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC )* 100 AS PEER_FOF_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_peer_rank(portfolio_name: str, end_date: str) -> pd.DataFrame:
    query_sql = f"""
        WITH a AS (
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            FROM
                portfolio_performance_inner a
                JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                AND a.END_DATE = b.END_DATE 
            WHERE
                1 = 1 
                AND a.END_DATE = '{end_date}' 
                AND ( a.TICKER_SYMBOL = b.PORTFOLIO_NAME OR b.PORTFOLIO_NAME = 'ALL' ) 
                AND a.TICKER_SYMBOL = '{portfolio_name}' UNION
            SELECT
                b.DATE_NAME,
                a.TICKER_SYMBOL,
                a.START_DATE,
                a.END_DATE,
                a.CUM_RETURN,
                a.ANNUAL_RETURN,
                a.ANNUAL_VOLATILITY,
                a.SHARP_RATIO_ANNUAL,
                a.CALMAR_RATIO_ANNUAL,
                a.MAXDD 
            	FROM
                    peer_performance_inner a
                    JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
                    AND a.END_DATE = b.END_DATE
                    JOIN peer_portfolio_type c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
                WHERE
                    1 = 1 
                    AND a.END_DATE = '{end_date}' 
                    AND c.PORTFOLIO_TYPE = '{portfolio_name}'
                    AND ( b.PORTFOLIO_NAME = '{portfolio_name}' OR b.PORTFOLIO_NAME = 'ALL' ) 
           ),
            b AS ( SELECT DATE_NAME, count(*) AS NUM FROM a GROUP BY DATE_NAME ) SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '累计收益率' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC ), '/', b.NUM ) AS PEER_PORTFOLIO_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CUM_RETURN DESC )* 100 AS PEER_PORTFOLIO_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '年化收益率' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC ), '/', b.NUM ) AS PEER_PORTFOLIO_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_RETURN DESC )* 100 AS PEER_PORTFOLIO_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
             '年化波动率' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY ), '/', b.NUM ) AS PEER_PORTFOLIO_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY ANNUAL_VOLATILITY )* 100 AS PEER_PORTFOLIO_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            '最大回撤' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD ), '/', b.NUM ) AS PEER_PORTFOLIO_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY MAXDD )* 100 AS PEER_PORTFOLIO_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
              '收益波动比' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_PORTFOLIO_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY SHARP_RATIO_ANNUAL DESC )* 100 AS PEER_PORTFOLIO_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME UNION
        SELECT
            a.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
              '年化收益回撤比' AS INDICATOR,
            CONCAT( ROW_NUMBER() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC ), '/', b.NUM ) AS PEER_PORTFOLIO_RANK,
            PERCENT_RANK() over ( PARTITION BY a.DATE_NAME ORDER BY CALMAR_RATIO_ANNUAL DESC )* 100 AS PEER_PORTFOLIO_RANK_PCT 
        FROM
            a
            JOIN b ON b.DATE_NAME = a.DATE_NAME
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_benchmark_value_outter(portfolio_name: str, end_date: str) -> pd.DataFrame:
    query_sql = f"""
    WITH a AS (
        SELECT
            b.DATE_NAME,
            a.TICKER_SYMBOL,
            a.START_DATE,
            a.END_DATE,
            a.CUM_RETURN,
            a.ANNUAL_RETURN,
            a.ANNUAL_VOLATILITY,
            a.SHARP_RATIO_ANNUAL,
            a.CALMAR_RATIO_ANNUAL,
            a.MAXDD 
        FROM
            benchmark_performance_inner a
            JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
            AND a.END_DATE = b.END_DATE 
        WHERE
            1 = 1 
            AND a.END_DATE = '{end_date}' 
            AND ( a.TICKER_SYMBOL = b.PORTFOLIO_NAME OR b.PORTFOLIO_NAME = 'ALL' ) 
            AND a.TICKER_SYMBOL = '{portfolio_name}' 
        ) SELECT
        a.DATE_NAME,
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        '累计收益率' AS INDICATOR,
        CUM_RETURN AS BENCHMARK_VALUE_OUTTER 
    FROM
        a UNION
    SELECT
        a.DATE_NAME,
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        '年化收益率' AS INDICATOR,
        ANNUAL_RETURN AS BENCHMARK_VALUE_OUTTER 
    FROM
        a UNION
    SELECT
        a.DATE_NAME,
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        '年化波动率' AS INDICATOR,
        ANNUAL_VOLATILITY AS BENCHMARK_VALUE_OUTTER 
    FROM
        a UNION
    SELECT
        a.DATE_NAME,
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        '最大回撤' AS INDICATOR,
        MAXDD AS BENCHMARK_VALUE_OUTTER 
    FROM
        a UNION
    SELECT
        a.DATE_NAME,
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        '收益波动比' AS INDICATOR,
        SHARP_RATIO_ANNUAL AS BENCHMARK_VALUE_OUTTER 
    FROM
        a UNION
    SELECT
        a.DATE_NAME,
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        '年化收益回撤比' AS INDICATOR,
        CALMAR_RATIO_ANNUAL AS BENCHMARK_VALUE_OUTTER 
    FROM
        a
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_portfolio_performance(portfolio_name: str, end_date: str) -> pd.DataFrame:
    """
    获取组合表现

    Parameters
    ----------
    portfolio_name : str
        组合名称
    end_date : str
        日期

    Returns
    -------
    pd.DataFrame
        _description_
    """
    # print(portfolio_name, end_date)
    portfolio_rank = get_portfolio_rank(
        portfolio_name=portfolio_name, end_date=end_date
    )
    portfolio_peer_median = (
        portfolio_rank.groupby(by=["DATE_NAME", "INDICATOR"])["PORTFOLIO_VALUE"]
        .median()
        .reset_index()
        .rename(columns={"PORTFOLIO_VALUE": "PEER_MEDIAN"})
    )
    portfolio_rank = portfolio_rank.query(f"TICKER_SYMBOL == '{portfolio_name}'")
    portfolio_rank = portfolio_rank.merge(
        portfolio_peer_median, on=["DATE_NAME", "INDICATOR"], how="left"
    )

    fof_rank = (
        get_fof_rank(portfolio_name=portfolio_name, end_date=end_date).query(
            f"TICKER_SYMBOL == '{portfolio_name}'"
        )
    )[["DATE_NAME", "INDICATOR", "PEER_FOF_RANK", "PEER_FOF_RANK_PCT"]]
    portfolio_rank = portfolio_rank.merge(
        fof_rank, on=["DATE_NAME", "INDICATOR"], how="left"
    )

    peer_rank = (
        get_peer_rank(portfolio_name=portfolio_name, end_date=end_date).query(
            f"TICKER_SYMBOL == '{portfolio_name}'"
        )
    )[["DATE_NAME", "INDICATOR", "PEER_PORTFOLIO_RANK", "PEER_PORTFOLIO_RANK_PCT"]]
    portfolio_rank = portfolio_rank.merge(
        peer_rank, on=["DATE_NAME", "INDICATOR"], how="left"
    )

    benchmark_outter = get_benchmark_value_outter(
        portfolio_name=portfolio_name, end_date=end_date
    )[["DATE_NAME", "INDICATOR", "START_DATE", "END_DATE", "BENCHMARK_VALUE_OUTTER"]]
    portfolio_rank = portfolio_rank.merge(
        benchmark_outter,
        on=["START_DATE", "END_DATE", "INDICATOR", "DATE_NAME"],
        how="left",
    )

    return portfolio_rank


def update_portfolio_performance(
    start_date: str, end_date: str, portfolio_name_list: list = None
) -> None:
    """
    更新组合表现及衍生组合表现

    Parameters
    ----------
    start_date : str
        开始日期
    end_date : str
        结束日期
    """
    trade_dates = dm.get_period_end_date(
        start_date=start_date, end_date=end_date, period="d"
    )
    portfolio_info = get_portfolio_info()
    portfolio_info["LISTED_DATE"] = portfolio_info["LISTED_DATE"].apply(
        lambda x: x.strftime(DATE_FORMAT)
    )

    for date in trade_dates:
        # print(date)
        # 写入自己计算的组合
        portfolio_names = portfolio_info.query(f"LISTED_DATE < '{date}'")[
            "PORTFOLIO_NAME"
        ].tolist()

        if portfolio_name_list is not None:
            portfolio_names = list(set(portfolio_names) & set(portfolio_name_list))
        with ProcessPoolExecutor() as executor:
            result_list = list(
                tqdm(
                    executor.map(
                        get_portfolio_derivatives_rank,
                        portfolio_names,
                        [date] * len(portfolio_names),
                    ),
                    total=len(portfolio_names),
                )
            )
        result_df = pd.concat(result_list)
        result_df.rename(columns={"DATE_NAME": "CYCLE"}, inplace=True)
        DB_CONN_JJTG_DATA.upsert(result_df, "portfolio_derivatives_performance")

        # 写入正式组合
        portfolio_names = portfolio_info.query(
            f"LISTED_DATE < '{date}' and IF_LISTED == 1 and PORTFOLIO_TYPE != '目标盈'"
        )["PORTFOLIO_NAME"].tolist()

        with ProcessPoolExecutor() as executor:
            result_list = list(
                tqdm(
                    executor.map(
                        get_portfolio_performance,
                        portfolio_names,
                        [date] * len(portfolio_names),
                    ),
                    total=len(portfolio_names),
                )
            )
        result_df = pd.concat(result_list)
        result_df.rename(columns={"DATE_NAME": "CYCLE"}, inplace=True)
        DB_CONN_JJTG_DATA.upsert(result_df, "portfolio_performance")


def query_portfolio_performance(trade_dt: str):
    query_sql = f"""
        SELECT
            TICKER_SYMBOL,
            CYCLE,
            START_DATE,
            END_DATE,
            INDICATOR,
            PORTFOLIO_VALUE,
            PEER_RANK,
            BENCHMARK_VALUE_OUTTER,
            BENCHMARK_VALUE_INNER,
            PEER_MEDIAN,
            PEER_PORTFOLIO_RANK
        FROM
            portfolio_performance 
        WHERE
            1 = 1 
            AND END_DATE = '{trade_dt}'
    """
    perf_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    trade_2d = dm.offset_trade_dt(trade_dt, 2)
    query_sql = f"""
        SELECT
            TICKER_SYMBOL ,
            CYCLE,
            INDICATOR,
            PEER_FOF_RANK 
        FROM
            portfolio_performance 
        WHERE
            1 = 1 
            AND END_DATE = '{trade_2d}'
        """
    fof_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    perf_df = perf_df.merge(
        fof_df, on=["TICKER_SYMBOL", "CYCLE", "INDICATOR"], how="left"
    )
    perf_df.rename(columns=RENAME_DICT, inplace=True)
    return perf_df


if __name__ == "__main__":
    hour = datetime.datetime.now().hour
    start_date = dm.offset_trade_dt(LAST_TRADE_DT, 2)
    end_date = LAST_TRADE_DT
    update_portfolio_performance(
        start_date=start_date,
        end_date=end_date,
    )
    # 如果当前时间大于10点，则发送邮件
    if 11 <= hour <= 15 and dm.if_trade_dt(TODAY):
        portfolio_performance = query_portfolio_performance(end_date)
        file_path = f"f:/BaiduNetdiskWorkspace/1-基金投研/2.1-监控/2-定时数据/组合监控数据/组合监控数据{end_date}.xlsx"
        portfolio_nav = query_portfolio_nav()
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            portfolio_performance.to_excel(writer, sheet_name="绩效表现")
            portfolio_nav.to_excel(writer, sheet_name="组合净值")

        mail_sender = MailSender()
        mail_sender.message_config(
            from_name="进化中的ChenGPT_0.1",
            subject=f"【每日监控】投顾组合数据监控{end_date}",
            file_path=file_path,
            content="详情请见附件",
        )
        mail_sender.send_mail()
