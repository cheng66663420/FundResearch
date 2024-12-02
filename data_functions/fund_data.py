# rootPath = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
# sys.path.append(rootPath)
from functools import reduce
import polars as pl
from quant_pl.pl_expr import rank_pct, rank_str
import pandas as pd
from joblib import Parallel, delayed

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.utils import cal_series_rank, cut_series_to_group

MULTI_COLUMNS_DICT = {
    "END_DATE": "BASIC_INFO",
    "TICKER_SYMBOL": "BASIC_INFO",
    "LEVEL": "BASIC_INFO",
    "LEVEL_1": "BASIC_INFO",
    "LEVEL_2": "BASIC_INFO",
    "LEVEL_3": "BASIC_INFO",
    "SEC_SHORT_NAME": "BASIC_INFO",
    "IS_MAIN": "BASIC_INFO",
    "IS_ILLIQUID": "BASIC_INFO",
    "ALPHA": "SCORE",
    "IR": "SCORE",
    "MAXDD": "SCORE",
    "VOL": "SCORE",
    "TOTAL_SCORE": "SCORE",
    "ALPHA_GROUP": "GROUP",
    "IR_GROUP": "GROUP",
    "MAXDD_GROUP": "GROUP",
    "VOL_GROUP": "GROUP",
    "TOTAL_SCORE_GROUP": "GROUP",
}


def query_fund_info(
    need_columns_list: list = [
        "TICKER_SYMBOL",
        "SEC_SHORT_NAME",
        "IS_MAIN",
        "IS_ILLIQUID",
    ],
) -> pd.DataFrame:
    """
    查询基金信息

    Parameters
    ----------
    need_columns_list : list, optional
        需要的字段名称,
        by default [ "TICKER_SYMBOL", "SEC_SHORT_NAME", "IS_MAIN", "IS_ILLIQUID" ]

    Returns
    -------
    pd.DataFrame
        _description_
    """
    need_columns = ",".join(need_columns_list)
    query_sql = f"""
    SELECT 
        {need_columns} 
    from 
        fund_info 
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_fund_fee(
    need_columns_list: list = [
        "TICKER_SYMBOL",
        "MANAGEMENT_FEE",
        "TRUSTEE_FEE",
        "SALE_FEE",
        "TOTAL_FEE",
        "7d",
        "30d",
    ],
) -> pd.DataFrame:
    """
    查询基金费率

    Parameters
    ----------
    need_columns_list : list, optional
        需要的字段, by default [ "TICKER_SYMBOL", "MANAGEMENT_FEE", "TRUSTEE_FEE", "SALE_FEE", "TOTAL_FEE", "7d", "30d", ]

    Returns
    -------
    pd.DataFrame
        费率DataFrame
    """
    need_columns = ",".join(need_columns_list)
    query_sql = f"""
    select
        {need_columns}
    from 
        view_fund_fee
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_basic_products(
    need_columns_list: list = [
        "TICKER_SYMBOL",
        "IF_IN_TRANCHE",
        "TRANCHE",
        "FIRST_BUY",
    ],
) -> pd.DataFrame:
    """
    查询基金投顾产品池情况

    Parameters
    ----------
    need_columns_list : list, optional
        需要查询的字段, by default [ "TICKER_SYMBOL", "IF_IN_TRANCHE", "TRANCHE", "FIRST_BUY", ]

    Returns
    -------
    pd.DataFrame
        _description_
    """
    need_columns = ",".join(need_columns_list)
    query_sql = f"""
    select
        {need_columns}
    from 
        portfolio_basic_products
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_fund_asset_own(
    date: str = None,
    need_columns_list: list = [
        "TICKER_SYMBOL",
        "NET_ASSET",
    ],
) -> pd.DataFrame:
    """
    根据日期查询最新的基金资产

    Parameters
    ----------
    date : str, optional
        日期, by default None
    need_columns_list : list, optional
        需要的字段, by default [ "TICKER_SYMBOL", "NET_ASSET", ]

    Returns
    -------
    pd.DataFrame
        _description_
    """
    if date is None:
        date = "20991231"
    need_columns = ",".join(need_columns_list)
    query_sql = f"""
    SELECT 
        {need_columns} 
    from 
        fund_asset_own 
    where 
        1=1
        and REPORT_DATE = (select max(REPORT_DATE) from fund_asset_own where PUBLISH_DATE<="{date}") 
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_fund_type_own(
    date: str = None,
    need_columns_list: list = ["TICKER_SYMBOL", "LEVEL_1", "LEVEL_2", "LEVEL_3"],
) -> pd.DataFrame:
    """
    根据日期查询最新的基金分类

    Parameters
    ----------
    date : str, optional
        日期, by default None
    need_columns_list : list, optional
        需要的字段, by default [ "TICKER_SYMBOL", "LEVEL_1", "LEVEL_2", "LEVEL_3" ]

    Returns
    -------
    pd.DataFrame
        _description_
    """
    if date is None:
        date = "20991231"
    need_columns = ",".join(need_columns_list)
    query_sql = f"""
    SELECT 
        {need_columns} 
    from 
        fund_type_own 
    where 
        1=1
        and REPORT_DATE = (select max(REPORT_DATE) from fund_asset_own where PUBLISH_DATE<="{date}") 
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_alpha_to_index(
    ticker_symbol: str, index_code: str, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    获取基金相对于宽基的超额收益率

    Parameters
    ----------
    ticker_symbol : str
        基金代码
    index_code : str
        指数代码
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    pd.DataFrame
        columns=[
            END_DATE, SEC_SHORT_NAME, LOG_RET,
            LOG_ALPHA_RET,SUM_ALPHA_RET
        ]
    """
    query_sql = f"""
    WITH b AS ( 
        SELECT 
            tradingDay as TRADE_DT, 
            (log( closePrice ) - log( PrevClosePrice ))*100 as LOG_RET
        FROM jy_indexquote 
        WHERE 
            SecuCode = '{index_code}'
    ) SELECT
    a.END_DATE,
    c.SEC_SHORT_NAME,
    a.LOG_RET,
    a.LOG_RET - b.LOG_RET AS LOG_ALPHA_RET,
    sum( a.LOG_RET) over ( PARTITION BY a.TICKER_SYMBOL ORDER BY a.END_DATE ) AS SUM_FUND_RET,
    sum( b.LOG_RET) over ( PARTITION BY a.TICKER_SYMBOL ORDER BY a.END_DATE ) AS SUM_INDEX_RET,
    sum( a.LOG_RET - b.LOG_RET ) over ( PARTITION BY a.TICKER_SYMBOL ORDER BY a.END_DATE ) AS SUM_ALPHA_RET 
    FROM
        `fund_adj_nav` a
        JOIN b ON a.END_DATE = b.TRADE_DT
        JOIN fund_info c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
    WHERE
        1 = 1 
        AND a.TICKER_SYMBOL = '{ticker_symbol}'
    """
    if start_date:
        query_sql += f"and a.END_DATE >= '{start_date}' "

    if end_date:
        query_sql += f"and a.END_DATE <= '{end_date}' "
    query_sql += " order by END_DATE"
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_fund_alpha_score(
    end_date: str, period: str = "3Y", level_num: int = 3
) -> pd.DataFrame:
    """
    获取基金超额收益得分

    Parameters
    ----------
    end_date : str
        结束日期
    period : str, optional
        周期, by default "3Y"

    Returns
    -------
    pd.DataFrame
        结果df
    """
    level_query = [f"LEVEL_{i}" for i in range(1, level_num + 1)]
    level_query = ",".join(level_query)
    query_sql = f"""
    WITH a AS (
        SELECT
            END_DATE,
            TICKER_SYMBOL,
            `LEVEL`,
            sum( CASE INDICATOR WHEN 'IR' THEN 3M ELSE 0 END ) AS IR_3M,
            sum( CASE INDICATOR WHEN 'IR' THEN 6M ELSE 0 END ) AS IR_6M,
            sum( CASE INDICATOR WHEN 'IR' THEN 9M ELSE 0 END ) AS IR_9M,
            sum( CASE INDICATOR WHEN 'IR' THEN 1Y ELSE 0 END ) AS IR_1Y,
            sum( CASE INDICATOR WHEN 'IR' THEN 2Y ELSE 0 END ) AS IR_2Y,
            sum( CASE INDICATOR WHEN 'IR' THEN 3Y ELSE 0 END ) AS IR_3Y,
            sum( CASE INDICATOR WHEN 'MAXDD' THEN 3M ELSE 0 END ) AS MAXDD_3M,
            sum( CASE INDICATOR WHEN 'MAXDD' THEN 6M ELSE 0 END ) AS MAXDD_6M,
            sum( CASE INDICATOR WHEN 'MAXDD' THEN 9M ELSE 0 END ) AS MAXDD_9M,
            sum( CASE INDICATOR WHEN 'MAXDD' THEN 1Y ELSE 0 END ) AS MAXDD_1Y,
            sum( CASE INDICATOR WHEN 'MAXDD' THEN 2Y ELSE 0 END ) AS MAXDD_2Y,
            sum( CASE INDICATOR WHEN 'MAXDD' THEN 3Y ELSE 0 END ) AS MAXDD_3Y,
            sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 3M ELSE 0 END ) AS VOL_3M,
            sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 6M ELSE 0 END ) AS VOL_6M,
            sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 9M ELSE 0 END ) AS VOL_9M,
            sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 1Y ELSE 0 END ) AS VOL_1Y,
            sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 2Y ELSE 0 END ) AS VOL_2Y,
            sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 3Y ELSE 0 END ) AS VOL_3Y,
            sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 3M ELSE 0 END ) AS ALPHA_3M,
            sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 6M ELSE 0 END ) AS ALPHA_6M,
            sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 9M ELSE 0 END ) AS ALPHA_9M,
            sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 1Y ELSE 0 END ) AS ALPHA_1Y,
            sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 2Y ELSE 0 END ) AS ALPHA_2Y,
            sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 3Y ELSE 0 END ) AS ALPHA_3Y 
        FROM
            fund_derivatives_fund_alpha_performance 
        WHERE
            1 = 1 
            AND END_DATE = "{end_date}" 
            AND {period} IS NOT NULL 
        GROUP BY
            END_DATE,
            TICKER_SYMBOL,
            `LEVEL` 
        ) SELECT
        a.END_DATE,
        a.TICKER_SYMBOL,
        a.LEVEL,
        {level_query},
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY IR_3M ) AS IR_3M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY IR_6M ) AS IR_6M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY IR_9M ) AS IR_9M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY IR_1Y ) AS IR_1Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY IR_2Y ) AS IR_2Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY IR_3Y ) AS IR_3Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY MAXDD_3M DESC ) AS MAXDD_3M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY MAXDD_6M DESC ) AS MAXDD_6M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY MAXDD_9M DESC ) AS MAXDD_9M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY MAXDD_1Y DESC ) AS MAXDD_1Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY MAXDD_2Y DESC ) AS MAXDD_2Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY MAXDD_3Y DESC) AS MAXDD_3Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY VOL_3M DESC ) AS VOL_3M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY VOL_6M DESC ) AS VOL_6M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY VOL_9M DESC ) AS VOL_9M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY VOL_1Y DESC ) AS VOL_1Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY VOL_2Y DESC ) AS VOL_2Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY VOL_3Y DESC) AS VOL_3Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY ALPHA_3M ) AS ALPHA_3M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY ALPHA_6M ) AS ALPHA_6M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY ALPHA_9M ) AS ALPHA_9M,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY ALPHA_1Y ) AS ALPHA_1Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY ALPHA_2Y ) AS ALPHA_2Y,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {level_query} ORDER BY ALPHA_3Y ) AS ALPHA_3Y 
    FROM
        a
        LEFT JOIN fund_type_own b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL
    WHERE
        1 = 1 
        AND b.PUBLISH_DATE = ( SELECT max( PUBLISH_DATE ) FROM fund_type_own WHERE PUBLISH_DATE <= "{end_date}"  ) 
        AND a.`LEVEL` = 'LEVEL_{level_num}' 
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_enhanced_index_fund_alpha_score(
    end_date: str, period: str = "3Y"
) -> pd.DataFrame:
    """
    查询指数增强基金的超额收益得分

    Parameters
    ----------
    end_date : str
        结束日期
    period : str, optional
        区间, by default "3Y"

    Returns
    -------
    pd.DataFrame
        结果df
    """
    query_sql = f"""
        WITH a AS (
            SELECT
                END_DATE,
                TICKER_SYMBOL,
                sum( CASE INDICATOR WHEN 'IR' THEN 3M ELSE 0 END ) AS IR_3M,
                sum( CASE INDICATOR WHEN 'IR' THEN 6M ELSE 0 END ) AS IR_6M,
                sum( CASE INDICATOR WHEN 'IR' THEN 9M ELSE 0 END ) AS IR_9M,
                sum( CASE INDICATOR WHEN 'IR' THEN 1Y ELSE 0 END ) AS IR_1Y,
                sum( CASE INDICATOR WHEN 'IR' THEN 2Y ELSE 0 END ) AS IR_2Y,
                sum( CASE INDICATOR WHEN 'IR' THEN 3Y ELSE 0 END ) AS IR_3Y,
                sum( CASE INDICATOR WHEN 'MAXDD' THEN 3M ELSE 0 END ) AS MAXDD_3M,
                sum( CASE INDICATOR WHEN 'MAXDD' THEN 6M ELSE 0 END ) AS MAXDD_6M,
                sum( CASE INDICATOR WHEN 'MAXDD' THEN 9M ELSE 0 END ) AS MAXDD_9M,
                sum( CASE INDICATOR WHEN 'MAXDD' THEN 1Y ELSE 0 END ) AS MAXDD_1Y,
                sum( CASE INDICATOR WHEN 'MAXDD' THEN 2Y ELSE 0 END ) AS MAXDD_2Y,
                sum( CASE INDICATOR WHEN 'MAXDD' THEN 3Y ELSE 0 END ) AS MAXDD_3Y,
                sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 3M ELSE 0 END ) AS VOL_3M,
                sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 6M ELSE 0 END ) AS VOL_6M,
                sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 9M ELSE 0 END ) AS VOL_9M,
                sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 1Y ELSE 0 END ) AS VOL_1Y,
                sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 2Y ELSE 0 END ) AS VOL_2Y,
                sum( CASE INDICATOR WHEN 'ANNUAL_VOL' THEN 3Y ELSE 0 END ) AS VOL_3Y,
                sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 3M ELSE 0 END ) AS ALPHA_3M,
                sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 6M ELSE 0 END ) AS ALPHA_6M,
                sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 9M ELSE 0 END ) AS ALPHA_9M,
                sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 1Y ELSE 0 END ) AS ALPHA_1Y,
                sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 2Y ELSE 0 END ) AS ALPHA_2Y,
                sum( CASE INDICATOR WHEN 'CUM_ALPHA' THEN 3Y ELSE 0 END ) AS ALPHA_3Y 
            FROM
                fund_derivatives_enhanced_index_performance 
            WHERE
                1 = 1 
                AND END_DATE = "{end_date}" 
                AND {period} IS NOT NULL 
            GROUP BY
                END_DATE,
                TICKER_SYMBOL 
            ) SELECT
            a.END_DATE,
            a.TICKER_SYMBOL,
            LEVEL_1, 
            LEVEL_2, 
            LEVEL_3,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY IR_3M ) AS IR_3M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY IR_6M ) AS IR_6M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY IR_9M ) AS IR_9M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY IR_1Y ) AS IR_1Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY IR_2Y ) AS IR_2Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY IR_3Y ) AS IR_3Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY MAXDD_3M DESC ) AS MAXDD_3M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY MAXDD_6M DESC ) AS MAXDD_6M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY MAXDD_9M DESC ) AS MAXDD_9M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY MAXDD_1Y DESC ) AS MAXDD_1Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY MAXDD_2Y DESC ) AS MAXDD_2Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY MAXDD_3Y DESC ) AS MAXDD_3Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY VOL_3M DESC ) AS VOL_3M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY VOL_6M DESC ) AS VOL_6M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY VOL_9M DESC ) AS VOL_9M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY VOL_1Y DESC ) AS VOL_1Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY VOL_2Y DESC ) AS VOL_2Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY VOL_3Y DESC ) AS VOL_3Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY ALPHA_3M ) AS ALPHA_3M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY ALPHA_6M ) AS ALPHA_6M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY ALPHA_9M ) AS ALPHA_9M,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY ALPHA_1Y ) AS ALPHA_1Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY ALPHA_2Y ) AS ALPHA_2Y,
            PERCENT_RANK() over ( PARTITION BY END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY ALPHA_3Y ) AS ALPHA_3Y 
        FROM
            a
            LEFT JOIN fund_type_own b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND b.PUBLISH_DATE = (
            SELECT
                max( PUBLISH_DATE ) 
            FROM
                fund_type_own 
            WHERE
            PUBLISH_DATE <= "{end_date}" 
            )
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def _cal_fund_alpha_score(
    end_date: str, period: str = "3Y", level_num: int = 3
) -> pd.DataFrame:
    """
    获取基金的超额收益得分

    Parameters
    ----------
    end_date : str
        结束日期
    period : str, optional
        周期, by default "3Y"
    level_num : int, optional
        分类层级, by default 3

    Returns
    -------
    pd.DataFrame
        结果DataFrame
    """
    fund_alpha_score = query_fund_alpha_score(
        end_date=end_date, period=period, level_num=level_num
    )
    level_list = [f"LEVEL_{i}" for i in range(1, level_num + 1)]
    columns = ["END_DATE", "TICKER_SYMBOL", "LEVEL"] + level_list
    fund_alpha_score = fund_alpha_score.set_index(columns)
    fund_alpha_score = (
        fund_alpha_score.stack()
        .reset_index()
        .rename(
            columns={f"level_{len(columns)}": "INDICATOR_NAME", 0: "INDICATOR_VALUE"}
        )
    )
    fund_alpha_score["TEMP"] = fund_alpha_score["INDICATOR_NAME"].str.split("_")
    fund_alpha_score["INDICATOR_NAME"] = fund_alpha_score["TEMP"].apply(lambda s: s[0])
    fund_alpha_score["INDICATOR_PERIOD"] = fund_alpha_score["TEMP"].apply(
        lambda s: s[1]
    )
    fund_alpha_score.drop(columns=["TEMP"], inplace=True)
    return fund_alpha_score


def cal_fund_alpha_total_score(
    end_date: str,
    period: str = "3Y",
    level_num: int = 3,
    indicator_name_weights: dict = {"IR": 0.3, "MAXDD": 0.2, "ALPHA": 0.3, "VOL": 0.2},
    indicator_period_weights: dict = {
        "3M": 0.2,
        "6M": 0.2,
        "1Y": 0.2,
        "2Y": 0.2,
        "3Y": 0.2,
    },
) -> pd.DataFrame:
    """
    计算基金超额收益总分

    Parameters
    ----------
    end_date : str
        结束日期
    period : str, optional
        周期, by default "3Y"
    level_num : int, optional
        分类层级, by default 3
    indicator_name_weights : _type_, optional
        指标权重, by default { "IR": 0.3, "MAXDD": 0.2, "ALPHA": 0.3, "VOL":0.2 }
    indicator_period_weights : _type_, optional
        指标周期权重, by default { "3M": 0.2, "6M": 0.2, "1Y": 0.3, "2Y":0.3 }

    Returns
    -------
    pd.DataFrame
        结果DataFrame
    """
    level_list = [f"LEVEL_{i}" for i in range(1, level_num + 1)]
    fund_alpha_score = _cal_fund_alpha_score(
        end_date=end_date, period=period, level_num=level_num
    )
    fund_alpha_score["INDICATOR_NAME_WEIGHT"] = fund_alpha_score["INDICATOR_NAME"].map(
        indicator_name_weights
    )
    fund_alpha_score["INDICATOR_PERIOD_WEIGHT"] = fund_alpha_score[
        "INDICATOR_PERIOD"
    ].map(indicator_period_weights)
    fund_alpha_score.dropna(inplace=True)
    columns = ["END_DATE", "TICKER_SYMBOL", "LEVEL"] + level_list
    fund_alpha_score["INDICATOR_SCORE"] = (
        fund_alpha_score["INDICATOR_PERIOD_WEIGHT"]
        * fund_alpha_score["INDICATOR_VALUE"]
    )
    fund_alpha_score["TOTAL_SCORE"] = (
        fund_alpha_score["INDICATOR_PERIOD_WEIGHT"]
        * fund_alpha_score["INDICATOR_NAME_WEIGHT"]
        * fund_alpha_score["INDICATOR_VALUE"]
    )
    fund_alpha_indicator_score = (
        fund_alpha_score.groupby(by=columns + ["INDICATOR_NAME"])["INDICATOR_SCORE"]
        .sum()
        .unstack()
    )

    fund_alpha_total_score = fund_alpha_score.groupby(by=columns)["TOTAL_SCORE"].sum()
    result_socre = fund_alpha_indicator_score.merge(
        fund_alpha_total_score, left_index=True, right_index=True
    )
    indicator_list = result_socre.columns
    result_socre = result_socre.reset_index()
    group_by = ["END_DATE"] + level_list
    result_socre_grouped = result_socre.groupby(by=group_by)
    result_df = []
    for _, grouped in result_socre_grouped:
        temp = grouped.copy()
        for indic in indicator_list:
            temp[f"{indic}_GROUP"] = cut_series_to_group(temp[indic])
        result_df.append(temp)
    result_df = pd.concat(result_df)
    result_df["CYCLE"] = period
    return result_df


def cal_enhanced_index_fund_alpha_total_score(
    end_date: str,
    period: str = "3Y",
    indicator_name_weights: dict = {"IR": 0.3, "MAXDD": 0.2, "ALPHA": 0.3, "VOL": 0.2},
    indicator_period_weights: dict = {
        "3M": 0.2,
        "6M": 0.2,
        "1Y": 0.2,
        "2Y": 0.2,
        "3Y": 0.2,
    },
) -> pd.DataFrame:
    """
    计算指数增强基金的超额收益得分

    Parameters
    ----------
    end_date : str
        结束日期
    period : str, optional
        区间, by default "3Y"
    indicator_name_weights : _type_, optional
        指标权重, by default { "IR": 0.3, "MAXDD": 0.2, "ALPHA": 0.3, "VOL":0.2 }
    indicator_period_weights : _type_, optional
        周期权重, by default { "3M": 0.2, "6M": 0.2, "1Y": 0.2, "2Y":0.2, "3Y":0.2 }

    Returns
    -------
    pd.DataFrame
        _description_
    """
    fund_alpha_score = query_enhanced_index_fund_alpha_score(
        end_date=end_date, period=period
    )

    columns = [
        "END_DATE",
        "TICKER_SYMBOL",
        "LEVEL_1",
        "LEVEL_2",
        "LEVEL_3",
    ]
    fund_alpha_score = fund_alpha_score.set_index(columns)

    fund_alpha_score = (
        fund_alpha_score.stack()
        .reset_index()
        .rename(
            columns={f"level_{len(columns)}": "INDICATOR_NAME", 0: "INDICATOR_VALUE"}
        )
    )
    fund_alpha_score["TEMP"] = fund_alpha_score["INDICATOR_NAME"].str.split("_")
    fund_alpha_score["INDICATOR_NAME"] = fund_alpha_score["TEMP"].apply(lambda s: s[0])
    fund_alpha_score["INDICATOR_PERIOD"] = fund_alpha_score["TEMP"].apply(
        lambda s: s[1]
    )
    fund_alpha_score.drop(columns=["TEMP"], inplace=True)
    fund_alpha_score["INDICATOR_NAME_WEIGHT"] = fund_alpha_score["INDICATOR_NAME"].map(
        indicator_name_weights
    )
    fund_alpha_score["INDICATOR_PERIOD_WEIGHT"] = fund_alpha_score[
        "INDICATOR_PERIOD"
    ].map(indicator_period_weights)
    fund_alpha_score.dropna(inplace=True)
    fund_alpha_score["INDICATOR_SCORE"] = (
        fund_alpha_score["INDICATOR_PERIOD_WEIGHT"]
        * fund_alpha_score["INDICATOR_VALUE"]
    )
    fund_alpha_score["TOTAL_SCORE"] = (
        fund_alpha_score["INDICATOR_PERIOD_WEIGHT"]
        * fund_alpha_score["INDICATOR_NAME_WEIGHT"]
        * fund_alpha_score["INDICATOR_VALUE"]
    )
    fund_alpha_indicator_score = (
        fund_alpha_score.groupby(by=columns + ["INDICATOR_NAME"])["INDICATOR_SCORE"]
        .sum()
        .unstack()
    )
    fund_alpha_total_score = fund_alpha_score.groupby(by=columns)["TOTAL_SCORE"].sum()
    result_socre = fund_alpha_indicator_score.merge(
        fund_alpha_total_score, left_index=True, right_index=True
    )
    indicator_list = result_socre.columns
    result_socre = result_socre.reset_index()
    group_by = [
        "END_DATE",
        "LEVEL_1",
        "LEVEL_2",
        "LEVEL_3",
    ]
    result_socre_grouped = result_socre.groupby(by=group_by)
    result_df = []
    for _, grouped in result_socre_grouped:
        temp = grouped.copy()
        for indic in indicator_list:
            temp[f"{indic}_GROUP"] = cut_series_to_group(temp[indic])
        result_df.append(temp)
    result_df = pd.concat(result_df)
    result_df["CYCLE"] = period
    return result_df


def query_fund_ret_rank(end_date: str) -> pd.DataFrame:
    """
    查询基金收益排名

    Parameters
    ----------
    end_date : str
        结束日期

    Returns
    -------
    pd.DataFrame
        columns=[
            TICKER_SYMBOL, END_DATE, '近1日', "近1月",
            "近3月", "近6月", "近1年", "YTD"
        ]
    """
    query_sql = f"""
    SELECT
		a.TICKER_SYMBOL,
		b.DATE_NAME,
		round( 
            PERCENT_RANK() over ( 
                PARTITION BY c.LEVEL_1, c.LEVEL_2, c.LEVEL_3, b.DATE_NAME
                ORDER BY CUM_RETURN DESC 
            ) * 100, 2 
        ) AS CUM_RET_RANK 
	FROM
		fund_performance_inner a
		JOIN portfolio_dates b ON a.START_DATE = b.START_DATE 
		AND a.END_DATE = b.END_DATE
		JOIN fund_type_own c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
	WHERE
		1 = 1 
		AND b.DATE_NAME IN ( '近1日', "近1月", "近3月", "近6月", "近1年", "YTD") 
		AND b.END_DATE = '{end_date}' 
		AND c.REPORT_DATE = ( SELECT max( REPORT_DATE ) FROM fund_type_own WHERE PUBLISH_DATE <= '{end_date}' ) 
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df = df.pivot_table(
        index="TICKER_SYMBOL", columns="DATE_NAME", values="CUM_RET_RANK"
    )
    df = df[["近1日", "近1月", "近3月", "近6月", "近1年", "YTD"]]
    return df.reset_index()


def query_fund_performance(trade_dt: str = None):
    query_sql = f"""
    SELECT
        a.DATE_NAME,
        b.END_DATE as TRADE_DT,
        b.TICKER_SYMBOL,
        b.CUM_RETURN as F_AVGRETURN,
        b.ANNUAL_VOLATILITY as F_STDARDDEV,
        b.SHARP_RATIO_ANNUAL as F_SHARPRATIO,
        b.MAXDD as F_MAXDOWNSIDE,
        b.CALMAR_RATIO_ANNUAL  as F_CALMAR
    FROM
        portfolio_dates a
        JOIN fund_performance_inner b ON a.END_DATE = b.END_DATE 
        AND a.START_DATE = b.START_DATE 
        AND a.PORTFOLIO_NAME = 'ALL' 
    WHERE
        1 = 1 
        AND b.END_DATE = '{trade_dt}'
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    rename_dict = {
        "近5年": "FIVEYEAR",
        "近4年": "FOURYEAR",
        "近3年": "THREEYEAR",
        "近2年": "TWOYEAR",
        "近1年": "YEAR",
        "近6月": "HALFYEAR",
        "YTD": "THISYEAR",
        "近3月": "QUARTER",
        "近1日": "DAY",
    }
    indicator_list = [
        "F_AVGRETURN_DAY",
        "F_AVGRETURN_THISYEAR",
        "F_AVGRETURN_QUARTER",
        "F_AVGRETURN_HALFYEAR",
        "F_AVGRETURN_YEAR",
        "F_AVGRETURN_TWOYEAR",
        "F_AVGRETURN_THREEYEAR",
        "F_AVGRETURN_FOURYEAR",
        "F_AVGRETURN_FIVEYEAR",
        "F_AVGRETURN_SIXYEAR",
        "F_STDARDDEV_HALFYEAR",
        "F_STDARDDEV_YEAR",
        "F_STDARDDEV_TWOYEAR",
        "F_STDARDDEV_THREEYEAR",
        "F_STDARDDEV_FIVEYEAR",
        "F_SHARPRATIO_HALFYEAR",
        "F_SHARPRATIO_YEAR",
        "F_SHARPRATIO_TWOYEAR",
        "F_SHARPRATIO_THREEYEAR",
        "F_MAXDOWNSIDE_THISYEAR",
        "F_MAXDOWNSIDE_QUARTER",
        "F_MAXDOWNSIDE_HALFYEAR",
        "F_MAXDOWNSIDE_YEAR",
        "F_MAXDOWNSIDE_TWOYEAR",
        "F_MAXDOWNSIDE_THREEYEAR",
        "F_CALMAR_THISYEAR",
        "F_CALMAR_QUARTER",
        "F_CALMAR_HALFYEAR",
        "F_CALMAR_YEAR",
        "F_CALMAR_TWOYEAR",
        "F_CALMAR_THREEYEAR",
    ]
    name_list = list(rename_dict.keys())
    df = df[df["DATE_NAME"].isin(name_list)]
    df["DATE_NAME"] = df["DATE_NAME"].map(rename_dict)
    df.set_index(["TICKER_SYMBOL", "TRADE_DT", "DATE_NAME"], inplace=True)
    df = df.stack().reset_index()
    df["INDICATOR"] = df.apply(lambda s: s["level_3"] + "_" + s["DATE_NAME"], axis=1)
    df = df[df["INDICATOR"].isin(indicator_list)]
    col = ["TICKER_SYMBOL", "TRADE_DT", "INDICATOR", 0]
    df = df[col].pivot_table(
        index=["TICKER_SYMBOL", "TRADE_DT"], columns="INDICATOR", values=0
    )
    df = df.reset_index()
    fund_type = dm.get_own_fund_type(trade_dt)[
        ["TICKER_SYMBOL", "LEVEL_1", "LEVEL_2", "LEVEL_3"]
    ]
    df = df.merge(fund_type)
    return df


def cal_fund_performance_rank(trade_dt: str, if_pct: bool = 1) -> pd.DataFrame:
    """
    计算基金表现排名

    Parameters
    ----------
    trade_dt : str
        交易日
    if_pct : bool, optional
        是否百分比, by default 1

    Returns
    -------
    pd.DataFrame
        基金表现排名
    """
    indicator_list = [
        "F_AVGRETURN_DAY",
        "F_AVGRETURN_THISYEAR",
        "F_AVGRETURN_QUARTER",
        "F_AVGRETURN_HALFYEAR",
        "F_AVGRETURN_YEAR",
        "F_AVGRETURN_TWOYEAR",
        "F_AVGRETURN_THREEYEAR",
        "F_AVGRETURN_FOURYEAR",
        "F_AVGRETURN_FIVEYEAR",
        "F_STDARDDEV_HALFYEAR",
        "F_STDARDDEV_YEAR",
        "F_STDARDDEV_TWOYEAR",
        "F_STDARDDEV_THREEYEAR",
        "F_STDARDDEV_FIVEYEAR",
        "F_SHARPRATIO_HALFYEAR",
        "F_SHARPRATIO_YEAR",
        "F_SHARPRATIO_TWOYEAR",
        "F_SHARPRATIO_THREEYEAR",
        "F_MAXDOWNSIDE_THISYEAR",
        "F_MAXDOWNSIDE_QUARTER",
        "F_MAXDOWNSIDE_HALFYEAR",
        "F_MAXDOWNSIDE_YEAR",
        "F_MAXDOWNSIDE_TWOYEAR",
        "F_MAXDOWNSIDE_THREEYEAR",
        "F_CALMAR_THISYEAR",
        "F_CALMAR_QUARTER",
        "F_CALMAR_HALFYEAR",
        "F_CALMAR_YEAR",
        "F_CALMAR_TWOYEAR",
        "F_CALMAR_THREEYEAR",
    ]

    df = query_fund_performance(trade_dt=trade_dt)
    cols = df.columns
    cols = [i for i in cols if i.startswith("F_")]
    df = pl.from_pandas(df).lazy()
    descending_dict = {
        "AVGRETURN": True,
        "STDARDDEV": False,
        "SHARPRATIO": True,
        "MAXDOWNSIDE": False,
        "CALMAR": True,
    }
    func_dict = {1: rank_pct, 0: rank_str}
    result_list = []
    for i in range(1, 4):
        partion_by = ["TRADE_DT"] + [f"LEVEL_{j}" for j in range(1, i + 1)]
        expr_list = [
            pl.col("TICKER_SYMBOL"),
            pl.col("TRADE_DT"),
            pl.lit(f"LEVEL_{i}").alias("LEVEL"),
        ]
        for indicator in indicator_list:
            # if indicator not in cols:
            #     continue
            descending_condition = descending_dict[indicator.split("_")[1]]
            expr_list.append(
                func_dict[if_pct](
                    indicator,
                    descending=descending_condition,
                    patition_by=partion_by,
                ).alias(indicator)
            )
        result_list.append(df.select(expr_list))
    return pl.concat(result_list).collect().to_pandas()


def query_fund_performance_rank_pct(trade_dt: str = None):
    query_sql = f"""
    SELECT 
        TRADE_DT,
        TICKER_SYMBOL,
        LEVEL,
        F_AVGRETURN_DAY,
        F_AVGRETURN_THISYEAR,
        F_AVGRETURN_QUARTER,
        F_AVGRETURN_HALFYEAR,
        F_AVGRETURN_YEAR,
        F_AVGRETURN_TWOYEAR,
        F_AVGRETURN_THREEYEAR,
        F_AVGRETURN_FOURYEAR,
        F_AVGRETURN_FIVEYEAR,
        F_AVGRETURN_SIXYEAR,
        F_STDARDDEV_HALFYEAR,
        F_STDARDDEV_YEAR,
        F_STDARDDEV_TWOYEAR,
        F_STDARDDEV_THREEYEAR,
        F_STDARDDEV_FIVEYEAR,
        F_SHARPRATIO_HALFYEAR,
        F_SHARPRATIO_YEAR,
        F_SHARPRATIO_TWOYEAR,
        F_SHARPRATIO_THREEYEAR,
        F_MAXDOWNSIDE_THISYEAR,
        F_MAXDOWNSIDE_QUARTER,
        F_MAXDOWNSIDE_HALFYEAR,
        F_MAXDOWNSIDE_YEAR,
        F_MAXDOWNSIDE_TWOYEAR,
        F_MAXDOWNSIDE_THREEYEAR,
        F_CALMAR_THISYEAR,
        F_CALMAR_QUARTER,
        F_CALMAR_HALFYEAR,
        F_CALMAR_YEAR,
        F_CALMAR_TWOYEAR,
        F_CALMAR_THREEYEAR
    FROM 
        fund_performance_rank_pct
    where
        1=1
        and TRADE_DT = "{trade_dt}"
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


if __name__ == "__main__":
    import datetime

    # t1 = datetime.datetime.now()
    # b = cal_fund_performance_rank("20231220")
    # b = b.query("TICKER_SYMBOL == '005156'")
    # t2 = datetime.datetime.now()
    # print(t2 - t1)
    # b.to_excel("D:/aaaaaa.xlsx")
    # df = cal_fund_alpha_total_score(
    #     end_date="20240117",
    #     period="1Y",
    #     indicator_name_weights={
    #         "IR": 0.3,
    #         "ALPHA": 0.5,
    #         "MAXDD": 0.2,
    #     },
    #     indicator_period_weights={
    #         "3M": 0.2,
    #         "6M": 0.3,
    #         "1Y": 0.5,
    #     },
    # )
    # df = (
    #     df.merge(query_fund_info(), how="left")
    #     .merge(query_fund_asset_own(), how="left")
    #     .merge(query_fund_fee(), how="left")
    #     .merge(query_basic_products(), how="left")
    # )
    # df["NET_ASSET"] = df["NET_ASSET"] / 100000000
    # (df.sort_values(by="TOTAL_SCORE", ascending=False).to_excel("D:/1Y期.xlsx"))
    # query = """select * from view_portfolio_products_daily_monitor"""
    # df = DB_CONN_JJTG_DATA.exec_query(query)
    # df1 = query_fund_ret_rank("20231130")
    # df.merge(df1).to_excel("D:/每日监控.xlsx")
    df = cal_fund_performance_rank_func("20241127", if_pct=0)
    print(df)
