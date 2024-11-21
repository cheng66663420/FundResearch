import contextlib

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.performance import Performance


# 计算数据
def _parallel_cal(
    ticker, df_grouped, dates_dict, indicator_name, indicator_func, if_est=False
):
    dates_dict_temp = dates_dict.copy()
    df_temp = df_grouped["NAV"]
    if "BENCHMARK_NAV" in df_grouped.columns:
        benchmark_temp = df_grouped["BENCHMARK_NAV"]
    else:
        benchmark_temp = pd.Series()

    result_dict = {indicator_name: {}}
    for date_name, date_tuple in dates_dict_temp.items():
        try:
            start_date, end_date = date_tuple
            fund_alpha_nav = df_temp[start_date:end_date].dropna()
            if not benchmark_temp.empty:
                benchmark_nav = benchmark_temp[start_date:end_date].dropna()
            else:
                benchmark_nav = pd.Series()
            # 数据是否以开始时间和结束时间结束
            if_condition = (
                fund_alpha_nav.empty
                or fund_alpha_nav.index[0] != start_date
                or fund_alpha_nav.index[-1] != end_date
            )
            if not if_condition:
                perf = Performance(
                    nav_series=fund_alpha_nav, benchmark_series=benchmark_nav
                )

            result_dict[indicator_name].update(
                {
                    date_name: (
                        None
                        if if_condition
                        else (getattr(perf, indicator_func[0])() * indicator_func[1])
                    )
                }
            )
        except Exception as e:
            print(e)

    if if_est:
        with contextlib.suppress(Exception):
            start_date, end_date = (df_temp.index[0], end_date)
            fund_alpha_nav = df_temp[start_date:end_date].dropna()

            benchmark_nav = benchmark_temp[start_date:end_date].dropna()
            # 数据是否以开始时间和结束时间结束
            if_condition = (
                fund_alpha_nav.empty
                or fund_alpha_nav.index[0] != start_date
                or fund_alpha_nav.index[-1] != end_date
            )

            if not if_condition:
                perf = Performance(
                    nav_series=fund_alpha_nav, benchmark_series=benchmark_nav
                )

            result_dict[indicator_name].update(
                {
                    "EST": (
                        None
                        if if_condition
                        else (getattr(perf, indicator_func[0])() * indicator_func[1])
                    )
                }
            )
    tmp_result = pd.DataFrame.from_dict(result_dict, orient="index")

    # if tmp_result.empty:
    #     return pd.DataFrame()
    tmp_result = (
        tmp_result.dropna(how="all")
        .where(tmp_result <= 10**8, np.inf)
        .where(tmp_result > -(10**8), -np.inf)
        .reset_index()
        .rename(columns={"index": "INDICATOR"})
    )
    tmp_result["END_DATE"] = end_date
    tmp_result["TICKER_SYMBOL"] = ticker
    return tmp_result


def _get_needed_dates_dict(date):
    dates_dict = dm.get_recent_period_end_date_dict(
        end_date=date,
        dates_dict={"m": [1, 2, 3, 6, 9], "y": [1, 2, 3]},
        if_cn=0,
    )

    dates_dict["YTD"] = (
        dm.get_last_peroid_end_date(end_date=date, period="y"),
        date,
    )
    dates_dict["QTD"] = (
        dm.get_last_peroid_end_date(end_date=date, period="q"),
        date,
    )
    dates_dict["MTD"] = (
        dm.get_last_peroid_end_date(end_date=date, period="m"),
        date,
    )
    dates_dict["WTD"] = (
        dm.get_last_peroid_end_date(end_date=date, period="w"),
        date,
    )

    return dates_dict


def cal_performance(end_date, start_date, indicator_dict, df, if_est=False):
    if start_date is None:
        start_date = end_date

    trade_dates = dm.get_period_end_date(
        start_date=start_date, end_date=end_date, period="d"
    )

    dates_dict_list = [_get_needed_dates_dict(date) for date in trade_dates]
    result_list = Parallel(n_jobs=-1, backend="multiprocessing")(
        delayed(_parallel_cal)(
            ticker, grouped_nav_df, dates_dict, indicator_name, indicator_func, if_est
        )
        for dates_dict in dates_dict_list
        for ticker, grouped_nav_df in df.groupby(by="TICKER_SYMBOL")
        for indicator_name, indicator_func in indicator_dict.items()
    )
    return pd.concat(result_list)


def cal_enhanced_index_performance(
    end_date: str,
    start_date: str = None,
    indicator_dict: dict = None,
    ticker_symbol_list=None,
) -> pd.DataFrame:
    """
    计算指数增强基金的业绩表现

    Parameters
    ----------
    end_date : str
        需要计算日期
    indicator_dict : dict, optional
        需要计算的指标, by default None

    Returns
    -------
    pd.DataFrame
        计算结果
    """
    if start_date is None:
        start_date = end_date

    if indicator_dict is None:
        indicator_dict = {
            "CUM_ALPHA": ["cum_returns_final", 100],
            "ANNUAL_ALPHA": ["annual_return", 100],
            "ANNUAL_VOL": ["annual_volatility", 100],
            "IR": ["sharpe_ratio", 1],
            "MAXDD": ["max_drawdown", 100],
        }
    start_date_temp = dm.offset_period_dt(trade_date=start_date, n=-4, period="y")
    # 获取指数增强基金的超额收益
    query_sql = f"""
    select 
        date_format(END_DATE, "%Y%m%d") as END_DATE, 
        TICKER_SYMBOL, 
        CUM_ALPHA_NAV as NAV
    from fund_derivatives_enhanced_index_alpha
    where 
        END_DATE BETWEEN DATE ( '{start_date_temp}' ) 
        AND DATE ('{end_date}')
    """
    if ticker_symbol_list is not None:
        ticker_symbol_str = ["'" + i + "'" for i in ticker_symbol_list]
        ticker_symbol_str = ",".join(ticker_symbol_str)
        query_sql += f"and TICKER_SYMBOL in ({ticker_symbol_str})"

    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df = df.set_index("END_DATE")
    if df.empty:
        return pd.DataFrame()

    return cal_performance(end_date, start_date, indicator_dict, df)


def cal_fund_inner_alpha_performance(
    end_date: str, start_date: str = None, indicator_dict: dict = None
) -> pd.DataFrame:
    """
    计算指数增强基金的业绩表现

    Parameters
    ----------
    end_date : str
        需要计算日期
    start_date: str:
        开始时间
    indicator_dict : dict, optional
        需要计算的指标, by default None

    Returns
    -------
    pd.DataFrame
        计算结果
    """
    if start_date is None:
        start_date = end_date

    if indicator_dict is None:
        indicator_dict = {
            "CUM_ALPHA": ["cum_returns_final", 100],
            "ANNUAL_ALPHA": ["annual_return", 100],
            "ANNUAL_VOL": ["annual_volatility", 100],
            "IR": ["sharpe_ratio", 1],
            "MAXDD": ["max_drawdown", 100],
        }
    start_date_temp = dm.offset_period_dt(trade_date=start_date, n=-4, period="y")
    end_date = dm.offset_trade_dt(end_date, 0)
    # 获取指数增强基金的超额收益
    query_sql = f"""
    SELECT
        date_format( a.END_DATE, "%Y%m%d" ) AS END_DATE,
        a.TICKER_SYMBOL,
        a.ALPHA_NAV_LEVEL_1,
        a.ALPHA_NAV_LEVEL_2,
        a.ALPHA_NAV_LEVEL_3,
        a.ALPHA_NAV_STYLE,
        a.ALPHA_NAV_BARRA 
    FROM
        fund_derivatives_fund_alpha a
    WHERE
        1 = 1 
        AND (
        a.END_DATE BETWEEN DATE ( '{start_date_temp}' ) 
        AND DATE ( '{end_date}' ))
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df = df.set_index("END_DATE")
    if df.empty:
        return pd.DataFrame()
    result_list = []
    for level in ["LEVEL_1", "LEVEL_2", "LEVEL_3", "STYLE", "BARRA"]:
        df_temp = df[["TICKER_SYMBOL", f"ALPHA_NAV_{level}"]]
        if df_temp.empty:
            continue
        df_temp.rename(columns={f"ALPHA_NAV_{level}": "NAV"}, inplace=True)
        result_temp = cal_performance(end_date, start_date, indicator_dict, df_temp)
        result_temp["LEVEL"] = level
        result_list.append(result_temp)
    df = pd.concat(result_list)
    df = df.set_index(["END_DATE", "TICKER_SYMBOL", "INDICATOR", "LEVEL"]).dropna()
    df = df.reset_index()
    return df


def cal_portfolio_performance(
    end_date: str, start_date: str = None, indicator_dict: dict = None
) -> pd.DataFrame:
    """
    计算组合业绩表现

    Parameters
    ----------
    end_date : str
        需要计算日期
    start_date: str:
        开始时间
    indicator_dict : dict, optional
        需要计算的指标, by default None

    Returns
    -------
    pd.DataFrame
        计算结果
    """
    if start_date is None:
        start_date = end_date
    if indicator_dict is None:
        indicator_dict = {
            "CUM_RETURN": ["cum_returns_final", 100],
            "ANNUAL_RETURN": ["annual_return", 100],
            "ANNUAL_VOL": ["annual_volatility", 100],
            "SR": ["sharpe_ratio", 1],
            "MAXDD": ["max_drawdown", 100],
            "CALMAR_RRATIO": ["calmar_ratio", 1],
            "IR": ["IR", 1],
            "ALPHA": ["alpha", 100],
            "ANNUAL_ALPHA": ["annual_alpha", 100],
            "BENCHMARK_RETURN": ["benchmark_cum_returns_finals", 100],
        }

    end_date = dm.offset_trade_dt(end_date, 0)
    # 获取指数增强基金的超额收益
    query_sql = f"""
    SELECT
        a.PORTFOLIO_NAME AS TICKER_SYMBOL,
        date_format( a.TRADE_DT, "%Y%m%d" ) AS END_DATE,
        ( a.PORTFOLIO_RET_ACCUMULATED / 100 + 1 ) AS NAV,
        ( 1+ a.BENCHMARK_RET_ACCUMULATED_INNER / 100 ) AS BENCHMARK_NAV 
    FROM
        portfolio_derivatives_ret a
        JOIN portfolio_derivatives_ret b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME
        JOIN portfolio_info c ON c.PORTFOLIO_NAME = a.PORTFOLIO_NAME 
    WHERE
        1 = 1 
        AND c.IF_LISTED = 1 
        AND c.PORTFOLIO_TYPE != '目标盈' 
        and b.TRADE_DT = '{end_date}'
        and a.TRADE_DT <= '{end_date}'
        ORDER BY
            a.TRADE_DT,
            a.PORTFOLIO_NAME
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df = df.set_index("END_DATE")
    if df.empty:
        return pd.DataFrame()

    result_temp = cal_performance(end_date, start_date, indicator_dict, df, if_est=1)
    result_temp = result_temp.set_index(
        [
            "END_DATE",
            "TICKER_SYMBOL",
            "INDICATOR",
        ]
    )
    result_temp = result_temp.reset_index()
    return result_temp


def cal_fund_inner_alpha_model(trade_dt: str):
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
            AND END_DATE = '{trade_dt}' 
        GROUP BY
            END_DATE,
            TICKER_SYMBOL,
            `LEVEL` 
        )
        ,b AS (
        SELECT
            a.END_DATE,
            a.TICKER_SYMBOL,
            b.LEVEL_1,
            b.LEVEL_2,
            b.LEVEL_3,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY IR_3M ) AS IR_3M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY IR_6M ) AS IR_6M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY IR_9M ) AS IR_9M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY IR_1Y ) AS IR_1Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY IR_2Y ) AS IR_2Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY IR_3Y ) AS IR_3Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY MAXDD_3M DESC ) AS MAXDD_3M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY MAXDD_6M DESC ) AS MAXDD_6M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY MAXDD_9M DESC ) AS MAXDD_9M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY MAXDD_1Y DESC ) AS MAXDD_1Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY MAXDD_2Y DESC ) AS MAXDD_2Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY MAXDD_3Y ) AS MAXDD_3Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY ALPHA_3M ) AS ALPHA_3M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY ALPHA_6M ) AS ALPHA_6M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY ALPHA_9M ) AS ALPHA_9M_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY ALPHA_1Y ) AS ALPHA_1Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY ALPHA_2Y ) AS ALPHA_2Y_SCORE,
            PERCENT_RANK() over ( PARTITION BY END_DATE, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3 ORDER BY ALPHA_3Y ) AS ALPHA_3Y_SCORE 
        FROM
            a
            JOIN fund_type_own b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL 
            join fund_info c on c.TICKER_SYMBOL = b.TICKER_SYMBOL
        WHERE
            1 = 1 
            AND a.IR_3Y IS NOT NULL 
            AND a.MAXDD_3Y IS NOT NULL 
            AND b.PUBLISH_DATE = ( SELECT max( PUBLISH_DATE ) FROM fund_type_own WHERE PUBLISH_DATE <= '20230421' ) 
            AND a.`LEVEL` = 'LEVEL_3' 
            AND b.LEVEL_1 IN ( "主动权益", '固收', "固收+" ) 
            and c.IS_MAIN=1 
            and ifnull(c.EXPIRE_DATE, "2099-12-31") >= '{trade_dt}' 
        ),
        c AS (
        SELECT
            END_DATE,
            TICKER_SYMBOL,
            LEVEL_1,
            LEVEL_2,
            LEVEL_3,
            round( IR_3M_SCORE * 0.2+ IR_6M_SCORE * 0.2+ IR_1Y_SCORE * 0.2+ IR_2Y_SCORE * 0.2 + IR_3Y_SCORE * 0.2, 4 )* 100 AS IR_SCORE,
            round( MAXDD_3M_SCORE * 0.2 + MAXDD_6M_SCORE * 0.2 + MAXDD_1Y_SCORE * 0.2+ MAXDD_2Y_SCORE * 0.2 + MAXDD_3Y_SCORE * 0.2, 4 )* 100 AS MAXDD_SCORE,
            round( ALPHA_3M_SCORE * 0.2 + ALPHA_6M_SCORE * 0.2 + ALPHA_1Y_SCORE * 0.2+ ALPHA_2Y_SCORE * 0.2 + ALPHA_3Y_SCORE * 0.2, 4 )* 100 AS ALPHA_SCORE 
        FROM
            b 
        ),
        e AS (
        SELECT
            DISTINCT a.TICKER_SYMBOL,
            1 AS "IF_MANAGER_CHANGE_6M"
        FROM
            `fund_manager_info` a
            JOIN fund_info b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
        WHERE
            a.POSITION = 'FM' 
            AND b.ESTABLISH_DATE < DATE_SUB( '{trade_dt}', INTERVAL - 6 MONTH ) AND a.DIMISSION_DATE >= DATE_SUB( '{trade_dt}', INTERVAL 6 MONTH ) 
            AND b.IS_MAIN = 1 
            AND b.EXPIRE_DATE IS NULL 
        ),
        d AS ( SELECT c.*, IR_SCORE * 0.4 + ALPHA_SCORE * 0.3 + MAXDD_SCORE * 0.2 AS TOTAL_SCORE FROM c WHERE 1 = 1 ) SELECT
        d.*,
        IFNULL(e.IF_MANAGER_CHANGE_6M,0) as IF_MANAGER_CHANGE_6M,
        NTILE( 10 ) over ( PARTITION BY d.END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY IR_SCORE DESC ) AS 'IR_GROUP',
        NTILE( 10 ) over ( PARTITION BY d.END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY MAXDD_SCORE DESC ) AS 'MAXDD_GROUP',
        NTILE( 10 ) over ( PARTITION BY d.END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY ALPHA_SCORE DESC ) AS 'ALPHA_GROUP',
        NTILE( 10 ) over ( PARTITION BY d.END_DATE, LEVEL_1, LEVEL_2, LEVEL_3 ORDER BY TOTAL_SCORE DESC ) AS 'TOTAL_SCORE_GROUP' 
    FROM
        d
        LEFT JOIN e ON e.TICKER_SYMBOL = d.TICKER_SYMBOL 
    WHERE
        1 = 1 
    ORDER BY
        d.END_DATE,
        LEVEL_1,
        LEVEL_2,
        LEVEL_3,
        TOTAL_SCORE DESC
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="fund_derivatives_inner_alpha_model")


if __name__ == "__main__":
    df = cal_fund_inner_alpha_model("20231211")
    # df.to_excel("d:/fund_inner_alpha_performance.xlsx")
