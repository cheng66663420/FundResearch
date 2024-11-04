# rootPath = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
# sys.path.append(rootPath)
import numpy as np
import pandas as pd

from quant_utils.db_conn import DB_CONN_JJTG_DATA


def get_portfolio_info() -> pd.DataFrame:
    """
    获取组合信息表

    Returns
    -------
    pd.DataFrame
        组合信息的df
    """
    query_sql = f"""
    SELECT
        * 
    FROM
        `chentiancheng`.`portfolio_info` 
    WHERE
        1 = 1 
    order by
        ORDER_ID,
        ID
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_portfolo_holding_new(portfolio_name: str = None) -> pd.DataFrame:
    """
    查询组合最新持仓

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None

    Returns
    -------
    pd.DataFrame
        最新持仓的DataFrame
    """
    query_sql = f"""
    SELECT
        PORTFOLIO_NAME,
        TICKER_SYMBOL,
        SEC_SHORT_NAME,
        WEIGHT 
    FROM
        `chentiancheng`.`view_portfolio_holding_new` 
    WHERE
        1=1
    """
    if portfolio_name is not None:
        query_sql += f" and PORTFOLIO_NAME = '{portfolio_name}'"
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_portfolio_daily_performance(end_date: str) -> pd.DataFrame:
    """
    查询基金每日表现

    Parameters
    ----------
    end_date : str
        日期

    Returns
    -------
    pd.DataFrame
        结果DataFrame
    """

    query_sql = f"""
    SELECT
        a.TICKER_SYMBOL as PORTFOLIO_NAME,
        a.END_DATE,
        a.CYCLE,
        a.PORTFOLIO_VALUE
    FROM
        portfolio_derivatives_performance a 
        join portfolio_info b on b.PORTFOLIO_NAME = a.TICKER_SYMBOL
    WHERE
        1 = 1 
        AND a.END_DATE = '{end_date}' 
        AND a.INDICATOR = '累计收益率'
        and ifnull(b.DELISTED_DATE, "20991231") >= a.END_DATE 
        and b.listed_date <= a.END_DATE
        and b.INCLUDE_QDII = 0 UNION
    SELECT
        a.TICKER_SYMBOL AS PORTFOLIO_NAME,
        a.END_DATE,
        a.CYCLE,
        a.PORTFOLIO_VALUE 
    FROM
        portfolio_derivatives_performance a
        JOIN portfolio_info b ON b.PORTFOLIO_NAME = a.TICKER_SYMBOL 
        join md_tradingdaynew c on c.PREV_TRADE_DATE = a.END_DATE
    WHERE
        1 = 1 
        AND c.TRADE_DT = '{end_date}' 
        AND a.INDICATOR = '累计收益率' 
        AND ifnull( b.DELISTED_DATE, "20991231" ) >= a.END_DATE 
        AND b.listed_date <= a.END_DATE 
        AND b.INCLUDE_QDII = 1
        and c.SECU_MARKET = 83
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df = df.pivot_table(
        index=["PORTFOLIO_NAME", "END_DATE"], columns="CYCLE", values="PORTFOLIO_VALUE"
    )
    columns = [
        "近1日",
        "近1周",
        "近1月",
        "近3月",
        "近6月",
        "YTD",
        "近1年",
        "成立日",
        "对客日",
    ]
    cols = df.columns.tolist()
    columns_list = [i for i in columns if i in cols]
    df = df[columns_list]
    df = df.reset_index()
    porfolio_info = get_portfolio_info()[
        [
            "PORTFOLIO_NAME",
            "ID",
            "ORDER_ID",
            "IF_LISTED",
            "LISTED_DATE",
            "TO_CLIENT_DATE",
        ]
    ]
    df = porfolio_info.merge(df)
    df = df.sort_values(
        by=["IF_LISTED", "ORDER_ID", "ID"], ascending=[False, True, True]
    )
    df.drop(columns=["ID", "IF_LISTED", "ORDER_ID"], inplace=True)

    maxdd_sql = f"""
    SELECT
        END_DATE,
        TICKER_SYMBOL AS PORTFOLIO_NAME,
        PORTFOLIO_VALUE AS `最大回撤` 
    FROM
        portfolio_derivatives_performance a
        JOIN portfolio_info b ON a.TICKER_SYMBOL = b.PORTFOLIO_NAME 
    WHERE
        1 = 1 
        AND `CYCLE` = '成立日' 
        AND `INDICATOR` = '最大回撤' 
        AND END_DATE = '{end_date}' 
        AND b.INCLUDE_QDII = 0 UNION
    SELECT
        END_DATE,
        TICKER_SYMBOL AS PORTFOLIO_NAME,
        PORTFOLIO_VALUE AS `最大回撤` 
    FROM
        portfolio_derivatives_performance a
        JOIN portfolio_info b ON a.TICKER_SYMBOL = b.PORTFOLIO_NAME
        JOIN md_tradingdaynew c ON c.PREV_TRADE_DATE = a.END_DATE 
    WHERE
        1 = 1 
        AND `CYCLE` = '成立日' 
        AND `INDICATOR` = '最大回撤' 
        AND c.TRADE_DT = '{end_date}' 
        AND b.INCLUDE_QDII = 1 
        AND c.SECU_MARKET = 83
    """
    maxdd_df = DB_CONN_JJTG_DATA.exec_query(maxdd_sql)
    df = df.merge(maxdd_df, how="left")

    new_high_sql = f"""
    WITH max AS ( 
    SELECT 
    PORTFOLIO_NAME, 
    max( PORTFOLIO_RET_ACCUMULATED ) AS MAX_RET 
    FROM 
    portfolio_derivatives_ret WHERE TRADE_DT <= '{end_date}' GROUP BY PORTFOLIO_NAME ) SELECT
    a.TRADE_DT AS END_DATE,
    a.PORTFOLIO_NAME,
    CASE
        
        WHEN PORTFOLIO_RET_ACCUMULATED >= max.MAX_RET THEN
        '是' ELSE '否' 
    END AS '是否新高' 
    FROM
    portfolio_derivatives_ret a
    JOIN max ON a.PORTFOLIO_NAME = max.PORTFOLIO_NAME
    JOIN portfolio_info b ON b.PORTFOLIO_NAME = a.PORTFOLIO_NAME 
    WHERE
    1 = 1 
    AND a.TRADE_DT = '{end_date}' 
    AND ifnull( b.DELISTED_DATE, "20991231" ) >= a.TRADE_DT 
    AND b.listed_date <= a.TRADE_DT AND b.INCLUDE_QDII = 0 UNION SELECT a.TRADE_DT AS END_DATE, a.PORTFOLIO_NAME, CASE WHEN PORTFOLIO_RET_ACCUMULATED >= max.MAX_RET THEN
        '是' ELSE '否' 
    END AS '是否新高' 
    FROM
    portfolio_derivatives_ret a
    JOIN max ON a.PORTFOLIO_NAME = max.PORTFOLIO_NAME
    JOIN portfolio_info b ON b.PORTFOLIO_NAME = a.PORTFOLIO_NAME
    JOIN md_tradingdaynew c ON c.PREV_TRADE_DATE = a.TRADE_DT 
    WHERE
    1 = 1 
    AND c.TRADE_DT = '{end_date}' 
    AND ifnull( b.DELISTED_DATE, "20991231" ) >= a.TRADE_DT 
    AND b.listed_date <= a.TRADE_DT 
    AND b.INCLUDE_QDII = 1 
    AND c.SECU_MARKET = 83
    """
    new_high = DB_CONN_JJTG_DATA.exec_query(new_high_sql)
    df = df.merge(new_high, how="left")
    df = df.set_index("END_DATE")
    return df


def query_portfolio_daily_evalution(
    end_date: str = None, portfolio_name: str = None
) -> pd.DataFrame:
    """
    查询组合每日评估得分

    Parameters
    ----------
    end_date : str
        日期
    portfolio_name: str
        组合名称
    Returns
    -------
    pd.DataFrame
        结果DataFrame
    """

    query_sql = """
        SELECT
            a.END_DATE,
            a.PORTFOLIO_NAME,
            b.PERSON_IN_CHARGE,
            b.PORTFOLIO_TYPE,
            b.LISTED_DATE,
            b.LEVEL_1,
            b.LEVEL_2,
            b.LEVEL_3,
            round(a.TOTAL_SCORE,2) as TOTAL_SCORE
        FROM
            portfolio_info b
            JOIN view_portfolio_evaluation a ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME
        WHERE
            1 = 1
        """
    if end_date is not None:
        query_sql += f' and a.END_DATE = "{end_date}"'
    if portfolio_name is not None:
        query_sql += f' and a.PORTFOLIO_NAME = "{portfolio_name}"'

    query_sql += "  order by b.ORDER_ID, b.ID"
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def query_portfolio_alpha(
    portfolio_name: str = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    查询组合超额收益率

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None
    start_date : str, optional
        起始时间, by default None
    end_date : str, optional
        结束时间, by default None

    Returns
    -------
    pd.DataFrame
        columns = []
    """
    df = query_portfolio_derivatives_ret(
        portfolio_name=portfolio_name, start_date=start_date, end_date=end_date
    )
    df["ALPHA"] = (
        df["PORTFOLIO_NAV"] / df.loc[0, "PORTFOLIO_NAV"]
        - df["BENCHMARK_NAV"] / df.loc[0, "BENCHMARK_NAV"]
    )
    return df


def query_portfolio_alpha_to_index(
    portfolio_name: str, index_code: str, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    _summary_

    Parameters
    ----------
    portfolio_name : str
        组合名称
    index_code : str
        指数代码
    start_date : str, optional
        开始时间, by default None
    end_date : str, optional
        结束时间, by default None

    Returns
    -------
    pd.DataFrame
        _description_
    """
    if end_date is None:
        end_date = "20991231"
    if start_date is None:
        start_date = "20000101"
    query_sql = f"""
    WITH b AS ( 
        SELECT TRADE_DT, LOG_RET 
        FROM aindex_eod_prices 
        WHERE TICKER_SYMBOL = '{index_code}' 
        UNION 
        SELECT TRADE_DT, LOG_RET 
        FROM fund_index_eod 
        WHERE TICKER_SYMBOL = '{index_code}' 
    ) SELECT
        a.TRADE_DT,
        a.PORTFOLIO_NAME,
        a.LOG_RETURN_RATE AS PORTFOLIO_LOG_RET,
        b.LOG_RET AS BENCHMARK_LOG_RET
    FROM
        portfolio_derivatives_ret a
        JOIN b ON a.TRADE_DT = b.TRADE_DT 
    WHERE
        1 = 1 
        AND a.portfolio_name = '{portfolio_name}' 
        and a.TRADE_DT between '{start_date}' and "{end_date}"
    ORDER BY
        TRADE_DT
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df.loc[0, "PORTFOLIO_LOG_RET"] = 0
    df.loc[0, "BENCHMARK_LOG_RET"] = 0
    df["LOG_ALPHA_RET"] = df["PORTFOLIO_LOG_RET"] - df["BENCHMARK_LOG_RET"]
    df["PORTFOLIO_NAV"] = np.exp(df["PORTFOLIO_LOG_RET"].cumsum() / 100)
    df["BENCHMARK_NAV"] = np.exp(df["BENCHMARK_LOG_RET"].cumsum() / 100)
    df["ALPHA"] = np.exp(df["LOG_ALPHA_RET"].cumsum() / 100) - 1
    return df[["TRADE_DT", "PORTFOLIO_NAV", "BENCHMARK_NAV", "ALPHA"]]


def query_portfolio_nav(
    portfolio_name: str = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    查询组合净值

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None
    start_date : str, optional
        起始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    pd.DataFrame
        columns = [TRADE_DT, PORTFOLIO_NAME, PORTFOLIO_NAV, BENCHMARK_NAV]
    """
    if start_date is None:
        start_date = "20000101"
    if end_date is None:
        end_date = "20991231"

    query_sql = f"""
    SELECT
        a.TRADE_DT,
        a.PORTFOLIO_NAME,
        a.PORTFOLIO_NAV / c.PORTFOLIO_NAV AS PORTFOLIO_NAV, 
	    a.BENCHMARK_NAV / c.BENCHMARK_NAV as BENCHMARK_NAV
    FROM
        portfolio_nav a
        JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME
        JOIN portfolio_nav c ON c.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
    WHERE
        1 = 1 
        AND a.TRADE_DT >= b.LISTED_DATE 
        AND c.TRADE_DT = b.LISTED_DATE 
        AND (a.TRADE_DT BETWEEN "{start_date}" and "{end_date}")
    """
    if portfolio_name is not None:
        query_sql += f" AND a.PORTFOLIO_NAME = '{portfolio_name}'"

    query_sql += " ORDER BY a.PORTFOLIO_NAME, a.TRADE_DT"
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df_grouped = df.groupby(by=["PORTFOLIO_NAME"])
    result_list = []
    for _, val in df_grouped:
        temp = val.copy()
        temp.index = range(0, temp.shape[0])
        temp["PORTFOLIO_NAV"] = temp["PORTFOLIO_NAV"] / temp.loc[0, "PORTFOLIO_NAV"]
        temp["BENCHMARK_NAV"] = temp["BENCHMARK_NAV"] / temp.loc[0, "BENCHMARK_NAV"]
        result_list.append(temp)
    return pd.concat(result_list)


def query_portfolio_derivatives_ret(
    portfolio_name: str = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    查询组合自己计算的累计收益率

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None
    start_date : str, optional
        _description_, by default None
    end_date : str, optional
        _description_, by default None

    Returns
    -------
    pd.DataFrame
        columns=[
            TRADE_DT, PORTFOLIO_NAME,
            PORTFOLIO_RET_ACCUMULATED, BENCHMARK_RET_ACCUMULATED_INNER
        ]
    """
    if start_date is None:
        start_date = "20000101"
    if end_date is None:
        end_date = "20991231"

    query_sql = f"""
    SELECT
        a.TRADE_DT,
        a.PORTFOLIO_NAME,
        a.PORTFOLIO_RET_ACCUMULATED,
        a.BENCHMARK_RET_ACCUMULATED_INNER
    FROM
        portfolio_derivatives_ret a
        JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME
        JOIN portfolio_derivatives_ret c ON c.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
    WHERE
        1 = 1 
        AND a.TRADE_DT >= b.LISTED_DATE 
        AND c.TRADE_DT = b.LISTED_DATE 
        AND (a.TRADE_DT BETWEEN "{start_date}" and "{end_date}")
    """
    if portfolio_name is not None:
        query_sql += f" AND a.PORTFOLIO_NAME = '{portfolio_name}'"
    query_sql += " ORDER BY a.PORTFOLIO_NAME, a.TRADE_DT"
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df["PORTFOLIO_NAV"] = df["PORTFOLIO_RET_ACCUMULATED"] / 100 + 1
    df["BENCHMARK_NAV"] = df["BENCHMARK_RET_ACCUMULATED_INNER"] / 100 + 1
    df_grouped = df.groupby(by=["PORTFOLIO_NAME"])
    result_list = []
    for _, val in df_grouped:
        temp = val.copy()
        temp.index = range(0, temp.shape[0])
        temp["PORTFOLIO_NAV"] = temp["PORTFOLIO_NAV"] / temp.loc[0, "PORTFOLIO_NAV"]
        temp["BENCHMARK_NAV"] = temp["BENCHMARK_NAV"] / temp.loc[0, "BENCHMARK_NAV"]
        result_list.append(temp)
    return pd.concat(result_list)[
        ["TRADE_DT", "PORTFOLIO_NAME", "PORTFOLIO_NAV", "BENCHMARK_NAV"]
    ]


def query_portfolio_products_ret(end_date: str) -> pd.DataFrame:
    """
    查询组合持仓产品的收益率

    Parameters
    ----------
    end_date : str
        结束日期

    Returns
    -------
    pd.DataFrame
        columns=[
            PORTFOLIO_NAME, TICKER_SYMBOL, SEC_SHORT_NAME,
            LEVEL_1, LEVEL_2, LEVEL_3,
        ]
    """
    query_sql = f"""
        WITH a AS (
            SELECT
                a.SEC_SHORT_NAME AS SEC_SHORT_NAME,
                a.TICKER_SYMBOL AS TICKER_SYMBOL,
                group_concat( a.PORTFOLIO_NAME SEPARATOR ',' ) AS PORTFOLIO_NAME 
            FROM
                view_portfolio_holding_new a
                JOIN portfolio_info b ON b.PORTFOLIO_NAME = a.PORTFOLIO_NAME 
            WHERE
                1 = 1 
                AND b.IF_LISTED = 1 
            GROUP BY
                a.TRADE_DT,
                a.TICKER_SYMBOL,
                a.SEC_SHORT_NAME 
            ) SELECT
            a.PORTFOLIO_NAME,
            a.TICKER_SYMBOL,
            a.SEC_SHORT_NAME,
            c.LEVEL_1,
            c.LEVEL_2,
            c.LEVEL_3,
            b.RETURN_RATE_TO_PREV_DT as DAILY_RET
        FROM
            a
            JOIN fund_adj_nav b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
            JOIN fund_type_own c ON c.TICKER_SYMBOL = b.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND c.REPORT_DATE = ( SELECT max( fund_type_own.REPORT_DATE ) FROM fund_type_own ) 
            AND b.end_date = "{end_date}"
        ORDER BY
            c.LEVEL_1,
            c.LEVEL_2,
            b.RETURN_RATE_TO_PREV_DT DESC
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def monitor_target_portfolio():
    query_sql = """
        SELECT
        a.*,
        b.`累计收益率(止盈后)`,
        b.`年化收益率(止盈后)`,
        c.PORTFOLIO_VALUE AS '最大回撤',
        d.TEMPERATURE AS '权益温度',
        e.`债市温度计` 
        FROM
        `view_mointor_target_portfolio` a
        JOIN view_mointor_target_portfolio_accumulated b ON a.`组合名称` = b.`组合名称`
        LEFT JOIN portfolio_derivatives_performance c ON c.END_DATE = b.`交易日` 
        AND c.TICKER_SYMBOL = a.`组合名称` 
        AND c.`CYCLE` = '成立日' 
        AND c.`INDICATOR` = '最大回撤'
        JOIN view_temperature_stock d ON d.END_DATE = a.`运作起始日` 
        AND d.TICKER_SYMBOL = '000985'
        JOIN view_temperature_bond e ON e.`日期` = d.END_DATE 
        WHERE
        1 = 1 
        ORDER BY
        `是否止盈` DESC,
        `组合名称`,
        `运营结束日期`,
        `运作起始日`
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


if __name__ == "__main__":
    df = monitor_target_portfolio()
    print(df)
