import datetime
from typing import Union

import pandas as pd
from dateutil.parser import parse

from quant_utils.data_moudle import fund, utils
from quant_utils.db_conn import (
    DB_CONN_JJTG_DATA,
    DB_CONN_JY,
    DB_CONN_JY_LOCAL,
    DB_CONN_WIND,
)


def get_portfolio_derivatives_ret(
    start_date: str = None,
    end_date: str = None,
    portfolio_name: Union[str, list[str]] = None,
) -> pd.DataFrame:
    """
    获取组合自己计算的收益率

    Parameters
    ----------
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None
    portfolio_name : Union[str, list[str]], optional
        组合名称字符串或列表, by default None

    Returns
    -------
    pd.DataFrame
        columns = [TRADE_DT, PORTFOLIO_NAME, PORTFOLIO_RET_ACCUMULATED, BENCHMARK_RET_ACCUMULATED_INNER]
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    query_sql = f"""
        SELECT
            DATE_FORMAT( TRADE_DT, '%Y%m%d' ) AS TRADE_DT,
            PORTFOLIO_NAME,
            PORTFOLIO_RET_ACCUMULATED,
            BENCHMARK_RET_ACCUMULATED_INNER
        From
            portfolio_derivatives_ret
        where
            1=1
            and (TRADE_DT between '{start_date}' and '{end_date}')
        """

    if portfolio_name is not None:
        portfolio_name = utils.convert_list_to_str(portfolio_name)
        query_sql += f"and PORTFOLIO_NAME in ({portfolio_name})"

    query_sql += "order by PORTFOLIO_NAME, TRADE_DT"
    return DB_CONN_JJTG_DATA.exec_query(query_sql).set_index("TRADE_DT")


def get_peer_fund(portfolio_name: str) -> pd.DataFrame:
    """
    获取投顾组合的同类基金

    Parameters
    ----------
    portfolio_name : str
        组合名称

    Returns
    -------
    pd.DataFrame
        投顾组合同类基金名单, columns=['TICKER_SYMBOL']
    """

    fund_type = fund.get_own_fund_type()

    peer_portfolio_query_sql = f"""
    select 
        PEER_QUERY 
    from 
        portfolio_info
    where
        1=1
        and portfolio_name = "{portfolio_name}"
    """
    peer_portfolio_query = DB_CONN_JJTG_DATA.exec_query(peer_portfolio_query_sql)[
        "PEER_QUERY"
    ].values[0]
    return fund_type.query(peer_portfolio_query)[["TICKER_SYMBOL"]]


def get_peer_fof(portfolio_name: str) -> pd.DataFrame:
    """
    获取投顾组合的fof

    Parameters
    ----------
    portfolio_name : str
        组合名称

    Returns
    -------
    pd.DataFrame
        投顾组合同类FOF基金名单, columns=['TICKER_SYMBOL']
    """
    query_sql = f"""
    SELECT
        TICKER_SYMBOL
    FROM
        fof_type
    WHERE
        1=1
        and INNER_TYPE = "{portfolio_name}"
    """

    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_portfolio_info() -> pd.DataFrame:
    """
    获取组合信息

    Returns
    -------
    pd.DataFrame
        组合信息表数据
    """
    query_sql = """
    SELECT
        * 
    FROM
        portfolio_info 
    WHERE
        1 = 1 
    order by order_id
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df["LISTED_DATE"] = df["LISTED_DATE"].apply(
        lambda s: s.strftime("%Y%m%d") if s else ""
    )
    df["TO_CLIENT_DATE"] = df["TO_CLIENT_DATE"].apply(
        lambda s: s.strftime("%Y%m%d") if s else ""
    )
    return df


def get_peer_portfolio(portfolio_name: str):
    query_sql = f"""
    SELECT
        InnerCode AS TICKER_SYMBOL 
    FROM
        portfolio_type 
    WHERE
        1 = 1 
        AND portfolio_type != '' 
        AND PORTFOLIO_TYPE = '{portfolio_name}'
    """
    return DB_CONN_JY_LOCAL.exec_query(query_sql)


def get_peer_portfolio_nav(
    ticker_symbol: str = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    获取同类投顾组合的净值数据

    Parameters
    ----------
    ticker_symbol : str, optional
        基金代码, by default None
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    pd.DataFrame
        同类投顾组合净值数据,
        columns=['TICKER_SYMBOL', 'TRADE_DT', 'ADJUST_NAV']
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    query_sql = f"""
    SELECT
        InnerCode AS TICKER_SYMBOL,
        DATE_FORMAT( EndDate, '%Y%m%d' ) AS TRADE_DT,
        DataValue + 1 as ADJUST_NAV
    FROM
        mf_portfolioperform 
    WHERE
        1 = 1 
        and (EndDate BETWEEN '{start_date}' AND '{end_date}')
        and StatPeriod = 999
        AND IndicatorCode = 66
    """
    query_sql = utils.fill_sql_ticker_symbol(ticker_symbol, query_sql, "InnerCode")
    sql_order = "order by InnerCode, EndDate"
    # 获取复权单位净值
    fund_adj_nav = DB_CONN_JY_LOCAL.exec_query(query_sql + sql_order)
    fund_adj_nav["ADJUST_NAV"] = fund_adj_nav["ADJUST_NAV"].astype("float")
    return fund_adj_nav


# def get_portfolio_daily_limit() -> pd.DataFrame:
#     """
#     获取组合当日限额

#     Returns
#     -------
#     pd.DataFrame
#          组合名称  个人单日购买(万元)  机构单日购买(万元)  起购金额(元)
#     """
#     query_sql = """
#         SELECT
#             a.PORTFOLIO_NAME as 组合名称,
#             round( min( b.MAX_BUY_DAILY_INDIVIDUAL / a.WEIGHT * 100 )/ 10000, 2 ) AS '个人单日购买(万元)',
#             round( min( b.MAX_BUY__DAILY_INSTITUTION / a.WEIGHT * 100 )/ 10000, 2 ) AS '机构单日购买(万元)',
#             round( max( b.FIRST_BUY / a.WEIGHT * 100 ), 2 ) AS '起购金额(元)'
#         FROM
#             view_portfolio_holding_new a
#             JOIN portfolio_basic_products b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL
#         GROUP BY
#             a.PORTFOLIO_NAME
#     """
#     return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_portfolio_dates_name(portfolio_name: str, end_date: str) -> pd.DataFrame:
    """
    获取

    Parameters
    ----------
    portfolio_name : str
        组合名称
    end_date : str
        结束日期

    Returns
    -------
    pd.DataFrame
          日期名称  开始日期  结束日期
    """
    query_sql = f"""
    SELECT
        b.DATE_NAME,
        b.START_DATE,
        b.END_DATE 
    FROM
        portfolio_info a
        JOIN portfolio_dates b 
    WHERE
        1 = 1 
        AND b.END_DATE = '{end_date}' 
        AND a.PORTFOLIO_NAME = '{portfolio_name}' 
        AND b.START_DATE >= a.LISTED_DATE 
        AND b.PORTFOLIO_NAME IN (
        'ALL',
        '{portfolio_name}')
        AND b.DATE_NAME NOT IN ( '近1日' ) 
    ORDER BY
    START_DATE DESC
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_listed_portfolio_derivatives_ret(
    start_date: str = None,
    end_date: str = None,
    portfolio_name: Union[str, list[str]] = None,
) -> pd.DataFrame:
    """
    获取组合自己计算的收益率

    Parameters
    ----------
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None
    portfolio_name : Union[str, list[str]], optional
        组合名称字符串或列表, by default None

    Returns
    -------
    pd.DataFrame
        columns = [TRADE_DT, PORTFOLIO_NAME, PORTFOLIO_RET_ACCUMULATED, BENCHMARK_RET_ACCUMULATED_INNER]
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    query_sql = f"""
        SELECT
        DATE_FORMAT( a.TRADE_DT, '%Y%m%d' ) AS TRADE_DT,
        a.PORTFOLIO_NAME,
        a.PORTFOLIO_RET_ACCUMULATED,
        a.BENCHMARK_RET_ACCUMULATED_INNER 
        FROM
        portfolio_derivatives_ret a 
        join portfolio_info b on a.PORTFOLIO_NAME = b.PORTFOLIO_NAME
        WHERE
        1 = 1 
        AND (
        a.TRADE_DT BETWEEN '{start_date}' 
        AND '{end_date}')
        AND a.TRADE_DT <= ifnull(b.DELISTED_DATE, '20991231')
    """

    if portfolio_name is not None:
        portfolio_name = utils.convert_list_to_str(portfolio_name)
        query_sql += f"and a.PORTFOLIO_NAME in ({portfolio_name})"

    query_sql += "order by PORTFOLIO_NAME, TRADE_DT"
    return DB_CONN_JJTG_DATA.exec_query(query_sql).set_index("TRADE_DT")


def get_portfolio_derivatives_perfomance(
    portfolio_name: str, end_date: str
) -> pd.DataFrame:
    """
    获取组合自己计算的组合表现指标

    Parameters
    ----------
    portfolio_name : str
        组合名称
    end_date : str
        结束日期

    Returns
    -------
    pd.DataFrame
    """
    query_sql = f"""
    SELECT
        INDICATOR AS '指标',
        CYCLE AS '时间区间',
        START_DATE AS '开始日期',
        END_DATE AS '结束日期',
        concat( round( PORTFOLIO_VALUE, 2 ), "%" ) AS '组合',
        concat( round(BENCHMARK_VALUE_INNER,2), "%" ) AS '业绩比较基准',
        PEER_RANK AS '同类排名',
        concat(round(PEER_RANK_PCT,2), "%" ) AS `同类排名百分比` 
    FROM
        portfolio_derivatives_performance 
    WHERE
        1 = 1 
        AND END_DATE = '{end_date}' 
        AND TICKER_SYMBOL = '{portfolio_name}' 
        ORDER BY
        START_DATE DESC,
        INDICATOR DESC
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_portfolio_daily_limit() -> pd.DataFrame:
    """
    获取组合每日限额

    Returns
    -------
    pd.DataFrame
        组合每日限额
    """
    query_sql = """
        WITH a AS (
        SELECT
            a.PORTFOLIO_NAME,
            a.TICKER_SYMBOL,
            a.SEC_SHORT_NAME,
            a.WEIGHT,
        CASE
            WHEN b.MAX_BUY_DAILY_INDIVIDUAL >= ifnull( c.MAX_BUY_DAILY_INDIVIDUAL, 0 ) THEN
            b.MAX_BUY_DAILY_INDIVIDUAL ELSE c.MAX_BUY_DAILY_INDIVIDUAL + b.MAX_BUY_DAILY_INDIVIDUAL 
            END AS '限购',
            b.MAX_BUY_DAILY_INDIVIDUAL,
            b.MAX_BUY__DAILY_INSTITUTION,
            b.FIRST_BUY,
            b.DELAY_DATE,
            a.ALTERNATIVE_TICKER_SYMBOL,
            a.ALTERNATIVE_SEC_SHORT_NAME,
            d.7d,
            d.30d,
            d.90d 
        FROM
            view_portfolio_holding_new a
            JOIN portfolio_basic_products b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL
            LEFT JOIN portfolio_basic_products c ON a.ALTERNATIVE_TICKER_SYMBOL = c.TICKER_SYMBOL
            JOIN view_fund_fee d ON d.TICKER_SYMBOL = a.TICKER_SYMBOL 
        ) SELECT
        PORTFOLIO_NAME,
        round( min( `MAX_BUY_DAILY_INDIVIDUAL` / WEIGHT * 100 )/ 10000, 2 ) AS '个人单日购买(万元)',
        round( min( `MAX_BUY__DAILY_INSTITUTION` / WEIGHT * 100 )/ 10000, 2 ) AS '机构单日购买(万元)',
        round( min( `限购` / WEIGHT * 100 )/ 10000, 2 ) AS '个人单日购买(万元)(含备选)',
        round( min( `限购` / WEIGHT * 100 )/ 10000, 2 ) AS '机构单日购买(万元)(含备选)',
        round( max( FIRST_BUY / WEIGHT * 100 ), 2 ) AS '起购金额(元)',
        max( DELAY_DATE ) AS `延迟交收`,
        round( sum( WEIGHT * 7d )/ 100, 2 ) AS 7d,
        round( sum( WEIGHT * 30d )/ 100, 2 ) AS 30d,
        round( sum( WEIGHT * 90d )/ 100, 2 ) AS 90d 
        FROM
        a 
        GROUP BY
        PORTFOLIO_NAME
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_portfolio_income() -> pd.DataFrame:
    """
    获取组合创收

    Returns
    -------
    pd.DataFrame
        组合创收
    """
    query_sql = """
    WITH a AS ( SELECT min( MANAGEMENT_FEE_INDIVIDUAL ) AS MIN_MANAGEMENT_FEE_INDIVIDUAL, min( MANAGEMENT_FEE_INSTITUTION ) AS MIN_MANAGEMENT_FEE_INSTITUTION FROM portfolio_basic_products WHERE 1=1 and IF_IN_TRANCHE=1),
    b AS (
    SELECT
        holding.portfolio_name,
        holding.ticker_symbol,
        holding.SEC_SHORT_NAME,
        holding.WEIGHT * ( product.MANAGEMENT_FEE_INDIVIDUAL - a.MIN_MANAGEMENT_FEE_INDIVIDUAL ) * product.MANAGEMENT_FEE / 10000 AS `管理费分成抵扣_个人`,
        holding.WEIGHT * ( product.MANAGEMENT_FEE_INSTITUTION - a.MIN_MANAGEMENT_FEE_INSTITUTION ) * product.MANAGEMENT_FEE / 10000 AS `管理费分成抵扣_机构`,
        holding.WEIGHT * MIN_MANAGEMENT_FEE_INDIVIDUAL * MANAGEMENT_FEE / 10000 AS `个人产品管理费分成`,
        holding.WEIGHT * MIN_MANAGEMENT_FEE_INSTITUTION * MANAGEMENT_FEE / 10000 AS `机构产品管理费分成`,
        holding.WEIGHT * product.SALE_FEE / 100 AS `销售服务费` 
    FROM
        view_portfolio_holding_new holding
        JOIN portfolio_basic_products product ON holding.ticker_symbol = product.ticker_symbol
        JOIN portfolio_info info ON info.PORTFOLIO_NAME = holding.PORTFOLIO_NAME
        JOIN a 
    ),
    c AS (
    SELECT
        b.portfolio_name,
        round( sum( `管理费分成抵扣_个人` ), 4 ) AS `抵扣_个人`,
        round( sum( `管理费分成抵扣_机构` ), 4 ) AS `抵扣_机构`,
        round( sum( `个人产品管理费分成` ), 4 ) AS `管理费分成收入_个人`,
        round( sum( `机构产品管理费分成` ), 4 ) AS `管理费分成收入_机构`,
        round( sum( b.`销售服务费` ), 4 ) AS `销售服务费` 
    FROM
        b 
    GROUP BY
        b.portfolio_name 
    ) SELECT
    c.portfolio_name AS '组合名称',
    d.PORTFOLIO_MANAGEMENT_FEE AS `投顾管理费`,
    a.MIN_MANAGEMENT_FEE_INDIVIDUAL AS '最低管理费分成_个人',
    a.MIN_MANAGEMENT_FEE_INSTITUTION AS '最低管理费分成_机构',
    c.`管理费分成收入_个人`,
    c.`管理费分成收入_机构`,
    c.`销售服务费`,
    d.PORTFOLIO_MANAGEMENT_FEE + `管理费分成收入_个人` + `销售服务费` AS `总创收_个人`,
    d.PORTFOLIO_MANAGEMENT_FEE + `管理费分成收入_机构` + `销售服务费` AS `总创收_机构`,
    c.`抵扣_个人`,
    c.`抵扣_机构`,
    d.PORTFOLIO_MANAGEMENT_FEE - `抵扣_个人` AS `实际成本_个人`,
    d.PORTFOLIO_MANAGEMENT_FEE - `抵扣_机构` AS `实际成本_机构` 
    FROM
    c
    JOIN portfolio_info d ON c.PORTFOLIO_NAME = d.PORTFOLIO_NAME
    JOIN a 
    WHERE
    1 = 1 
    AND d.IF_LISTED = 1 
    ORDER BY
    d.ORDER_ID
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


__all__ = [
    "get_portfolio_derivatives_ret",
    "get_peer_fund",
    "get_peer_fof",
    "get_portfolio_info",
    "get_peer_portfolio",
    "get_peer_portfolio_nav",
    "get_portfolio_dates_name",
    "get_listed_portfolio_derivatives_ret",
    "get_portfolio_derivatives_perfomance",
    "get_portfolio_daily_limit",
    "get_portfolio_income",
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
