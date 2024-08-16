import datetime
from typing import Union

import pandas as pd
from dateutil.parser import parse

from quant_utils import constant
from quant_utils.data_moudle import utils
from quant_utils.db_conn import (
    DB_CONN_JJTG_DATA,
    DB_CONN_JY,
    DB_CONN_JY_LOCAL,
    DB_CONN_WIND,
)


def get_fund_risk_exposure(report_date: str) -> pd.DataFrame:
    """
    获取基金的风险敞口，包括权益+0.5*转债

    Parameters
    ----------
    report_date : str
        报告期.

    Returns
    -------
    pd.DataFrame
        基金的风险敞口.
    """

    query_sql = f"""
    SELECT
        REPORT_DATE,
        TICKER_SYMBOL,
        EQUITY_RATIO_IN_NA as EQUITY_RATIO_IN_NA,
        ifnull(F_PRT_COVERTBONDTONAV,0) AS COVERTBOND_RATIO_IN_NA,
        (
        IFNULL( EQUITY_RATIO_IN_NA, 0 ) + 0.5 * IFNULL( F_PRT_COVERTBONDTONAV, 0 )) AS RISK_EXPOSURE,
        ( IFNULL( EQUITY_RATIO_IN_NA, 0 ) + 0.0001 )/(
            0.5 * IFNULL( F_PRT_COVERTBONDTONAV, 0 ) + 0.0001 
        ) AS EQUITY_TO_COVERTBOND 
    FROM
    	fund_asset_own
    WHERE
    	REPORT_DATE = '{report_date}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_risk_exposure_period_mean(date: str, period_num: int = 4) -> pd.DataFrame:
    """
    获取指定date前n期的基金持仓风险暴露,风险暴露定义为权益占比+0.5*转债占比

    Parameters
    ----------
    date : str
        日期.
    period_num : int, optional
        滚动的期数. The default is 4.

    Returns
    -------
    fund_risk_exposure_mean :
    pd.DataFrame
        基金的风险暴露.
    """
    fund_risk_exposure = pd.DataFrame()

    # 滚动4期的报告期
    report_dates_list = utils.get_report_date(date, period_num)

    # 获取风险暴露
    list_risk_exposure = []
    for report_date in report_dates_list:
        temp_df = get_fund_risk_exposure(report_date)
        list_risk_exposure.append(temp_df)

    fund_risk_exposure = pd.concat(list_risk_exposure)

    return (
        fund_risk_exposure.groupby(by=["TICKER_SYMBOL"])[
            [
                "EQUITY_RATIO_IN_NA",
                "COVERTBOND_RATIO_IN_NA",
                "EQUITY_TO_COVERTBOND",
                "RISK_EXPOSURE",
            ]
        ]
        .mean()
        .reset_index()
    )


def get_sector_exposure(report_date: str) -> pd.DataFrame:
    """
    获取当前报告期的板块暴露
    Parameters
    ----------
    report_date : str
        报告期

    Returns
    -------
    pd.DataFrame
        板块暴露
    """
    # 查询语句
    query_sql = f"""
    SELECT
    	REPORT_DATE,
    	TICKER_SYMBOL,
    	SECTOR,
    	SECTOR_RATIO_IN_NA,
        SECTOR_RATIO_IN_EQUITY
    FROM
    	fund_holding_sector
    WHERE
    	REPORT_DATE = '{report_date}'
    """

    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_sector_exposure_period_mean(date: str, period_num: int = 4) -> pd.DataFrame:
    """
    获取指定日期的前N个季度的板块暴露

    Parameters
    ----------
    date : str
        报告期.
    period_num : int, optional
        采用的报告期数. The default is 4.

    Returns
    -------
    fund_sector_exposure_mean :
    pd.DataFrame
        基金板块暴露n期平均.
    """
    fund_sector_exposure = pd.DataFrame()

    # 滚动前4期的报告期
    report_dates_list = utils.get_report_date(date, period_num)

    # 获取报告期的板块数据
    list_sector_exposure = []
    for report_date in report_dates_list:
        temp_df = get_sector_exposure(report_date)
        list_sector_exposure.append(temp_df)
    fund_sector_exposure = pd.concat(list_sector_exposure)

    return (
        fund_sector_exposure.groupby(by=["TICKER_SYMBOL", "SECTOR"])[
            ["SECTOR_RATIO_IN_NA", "SECTOR_RATIO_IN_EQUITY"]
        ]
        .mean()
        .reset_index()
    )


def get_hk_exposure(report_date: str) -> pd.DataFrame:
    """
    获取港股暴露

    Parameters
    ----------
    report_date : str
        报告期.

    Returns
    -------
    pd.DataFrame
        港股持仓暴露.
    """

    query_sql = f"""
    SELECT
    	REPORT_DATE,
    	TICKER_SYMBOL,
    	IFNULL(F_PRT_HKSTOCKVALUETONAV,0) as F_PRT_HKSTOCKVALUETONAV
    FROM
    	fund_asset_own
    WHERE
    	REPORT_DATE = '{report_date}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_hk_exposure_period_mean(date: str, period_num: int = 4) -> pd.DataFrame:
    """
    获取过去N期的港股占比

    Parameters
    ----------
    date : str
        开始日期
    period_num : int, optional
        需要的N期数, by default 4

    Returns
    -------
    DataFrame
        过去N期的港股占比,
        columns=['TICKER_SYMBOL', 'F_PRT_HKSTOCKVALUETONAV']
    """

    # 滚动前4期的报告期
    report_dates_list = utils.get_report_date(date, period_num)

    # 获取报告期的板块数据
    list_hk_exposure = []
    for report_date in report_dates_list:
        temp_df = get_hk_exposure(report_date)
        list_hk_exposure.append(temp_df)
    hk_exposure = pd.concat(list_hk_exposure)
    hk_exposure = (
        hk_exposure.groupby(by=["TICKER_SYMBOL"])["F_PRT_HKSTOCKVALUETONAV"]
        .mean()
        .reset_index()
    )
    return hk_exposure


def get_fund_stocks_holding(
    ticker_symbol: str = None, report_date: str = None
) -> pd.DataFrame:
    """
    从datayes数据库提取基金股票持仓,筛选条件(1)基金代码(2)报告期，二者至少选其一

    Parameters
    ----------
    ticker_symbol : str, optional
        基金代码, by default None
    report_date : str, optional
        报告期, by default None

    Returns
    -------
    pd.DataFrame
        基金权益持仓, 包含MARKET_VALUE, RATIO_IN_NA
    """

    query_sql = """
    SELECT
		a.REPORT_DATE,
		b.TICKER_SYMBOL,
		b.SEC_SHORT_NAME,
		a.HOLDING_TICKER_SYMBOL as STOCK_TICKER_SYMBOL,
        a.MARKET_VALUE,
		a.RATIO_IN_NA
	FROM
		fund_holdings a
        join fund_info b on b.FUND_ID = a.FUND_ID
    WHERE
		SECURITY_TYPE = 'E' /*限制证券类型为股票*/
    """

    if report_date:
        query_sql += f'and a.REPORT_DATE = "{report_date}"'
    if ticker_symbol:
        query_sql += f'and b.TICKER_SYMBOL = "{ticker_symbol}"'

    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_adj_nav(
    ticker_symbol: str = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    获取所有类型基金的复权净值

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
        基金复权单位净值.columns=[TICKER_SYMBOL, TRADE_DT, ADJUST_NAV]
    """

    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    query_sql = f"""
    SELECT
        TICKER_SYMBOL,
        DATE_FORMAT(END_DATE, '%Y%m%d')  as TRADE_DT,
        ADJ_NAV AS ADJUST_NAV
    FROM
        fund_adj_nav
    WHERE
        1 = 1
        and (END_DATE BETWEEN '{start_date}' AND '{end_date}')
    """
    query_sql = utils.fill_sql_ticker_symbol(ticker_symbol, query_sql, "TICKER_SYMBOL")
    sql_order = "order by TICKER_SYMBOL, END_DATE"
    # 获取复权单位净值
    fund_adj_nav = DB_CONN_JJTG_DATA.exec_query(query_sql + sql_order)
    fund_adj_nav["ADJUST_NAV"] = fund_adj_nav["ADJUST_NAV"].astype("float")
    return fund_adj_nav


def get_fund_nav_growth(
    end_date: str = None, ticker_symbol: str = None
) -> pd.DataFrame:
    """
    查询某天获取基金净值增长情况

    Parameters
    ----------
    end_date : str
        日期字符串
    ticker_symbol : str
        基金代码
    Returns
    -------
    pd.DataFrame
        基金净值增长的DataFrame
    """
    query_sql = """
    SELECT
        a.TICKER_SYMBOL,
        b.SEC_SHORT_NAME,
        a.END_DATE,
        a.RETURN_RATE,
        a.RETURN_RATE_WTD,
        a.RETURN_RATE_1W,
        a.RETURN_RATE_MTD,
        a.RETURN_RATE_1M,
        a.RETURN_RATE_3M,
        a.RETURN_RATE_6M,
        a.RETURN_RATE_YTD,
        a.RETURN_RATE_1Y,
        a.RETURN_RATE_2Y,
        a.RETURN_RATE_3Y
    FROM
        fund_nav_gr a
        JOIN fund_info b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL 
        AND (
            a.END_DATE BETWEEN b.ESTABLISH_DATE 
        AND IFNULL( b.EXPIRE_DATE, "2099-12-31" )) 
    WHERE
        1=1
    """

    # 当结束日期不为空
    if end_date is not None:
        query_sql += f"and a.END_DATE ='{end_date}'"

    if ticker_symbol is not None:
        query_sql += f"and a.TICKER_SYMBOL='{ticker_symbol}'"

    query_sql += "ORDER BY a.TICKER_SYMBOL, a.END_DATE"

    result = DB_CONN_JJTG_DATA.exec_query(query_sql)
    # None替换为nan
    result = result.where(result.notnull(), np.nan)
    return result


def get_fund_redeem_fee() -> pd.DataFrame:
    """
    获取基金不同区间范围的赎回费用，
    默认区间范围[7, 30, 90, 180, 270, 365, 365*2, 365*3, 365*4]
    Returns
    -------
    pd.DataFrame
        基金赎回费的DataFrame
    """

    def __get_sql(date_unit, date) -> str:
        """获取查询赎回费的sql语句,主要更新查询时间单位，与日期"""
        query_sql = f"""
        SELECT
            b.TICKER_SYMBOL,
            b.SEC_SHORT_NAME,
            MAX_CHAR_RATE
        FROM
            fund_fee_new a
            JOIN fund_info b ON b.SECURITY_ID = a.SECURITY_ID 
        WHERE
            IS_EXE = 1 /*正在执行的费率*/
            AND CLIENT_TYPE = 10 /*01为一般投资者*/
            AND CHARGE_TYPE in (12000, 12200)
            AND (CHAR_CON_UNIT1 = { date_unit } or CHAR_CON_UNIT1 is null)
            AND ( CHAR_START1 <= { date } OR CHAR_START1 IS NULL ) 
            AND ( CHAR_END1 > { date } OR CHAR_END1 IS NULL ) 
            AND b.SEC_SHORT_NAME NOT LIKE "%定开%" 
            AND b.SEC_SHORT_NAME NOT LIKE "%持有%" 
        ORDER BY
            TICKER_SYMBOL
        """
        return query_sql

    dates = [1, 7, 30, 60, 90, 180, 270, 365, 365 * 2, 365 * 3, 365 * 4]

    date_dict = {"月": 2, "日": 3, "年": 1}

    result_list = []
    for date in dates:
        dates_dict = {}

        temp_date = date + 1
        date_year = date / 365
        dates_dict["年"] = date_year

        date_month = temp_date / 30
        dates_dict["月"] = int(date_month)

        dates_dict["日"] = temp_date + 1

        for k, val in dates_dict.items():
            query_sql = __get_sql(date_unit=date_dict[k], date=val)

            df = DB_CONN_JJTG_DATA.exec_query(query_sql)[
                ["TICKER_SYMBOL", "SEC_SHORT_NAME", "MAX_CHAR_RATE"]
            ]

            if not df.empty:
                df["持有期"] = date
                result_list.append(df)

    result = pd.concat(result_list)

    result["MAX_CHAR_RATE"] = result["MAX_CHAR_RATE"].astype("float")
    result = result.pivot_table(
        index="TICKER_SYMBOL", columns="持有期", values="MAX_CHAR_RATE", aggfunc="max"
    )
    result.columns = [f"{col}d" for col in result.columns]
    result = result.reset_index().drop_duplicates(subset=["TICKER_SYMBOL"])

    return result


def get_own_fund_type(report_date: str = None) -> pd.DataFrame:
    """
    根据report_date获取内部基金分类,如果report_date为None,则取最新报告期数据

    Parameters
    ----------
    report_date : str, optional
        报告期, by default None

    Returns
    -------
    pd.DataFrame
        内部基金分类的结果
    """

    sql_query = """
        SELECT
            *
        FROM
            fund_type_own
    """

    if report_date is None:
        sql_query += """
        WHERE REPORT_DATE = (SELECT max(REPORT_DATE) FROM fund_type_own)
        """
    else:
        sql_query += f"WHERE REPORT_DATE = (SELECT max(REPORT_DATE) FROM fund_type_own where PUBLISH_date <= '{report_date}') "

    return DB_CONN_JJTG_DATA.exec_query(sql_query)


def get_fund_index(
    ticker_symbol: str = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    获取基金指数收盘价

    Parameters
    ----------
    ticker_symbol : str, optional
        基金6位的代码, by default None
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
        columns=[TRADE_DT, TICKER_SYMBOL, S_DQ_CLOSE]
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)

    sql_query = f"""
    SELECT
        DATE_FORMAT( TRADE_DT, '%Y%m%d' ) AS TRADE_DT,
        TICKER_SYMBOL,
        S_DQ_CLOSE
    FROM
        fund_index_eod
    WHERE
        TRADE_DT between '{start_date}' and '{end_date}'
    """

    if ticker_symbol is not None:
        ticker_symbol = utils.convert_list_to_str(ticker_symbol)
        sql_query += f"and  TICKER_SYMBOL in ({ticker_symbol})"
    sql_order = "order by TICKER_SYMBOL, TRADE_DT"

    # 获取复权单位净值
    fund_adj_nav = DB_CONN_JJTG_DATA.exec_query(sql_query + sql_order)
    fund_adj_nav["S_DQ_CLOSE"] = fund_adj_nav["S_DQ_CLOSE"].astype("float")
    return fund_adj_nav


def get_fund_barra_sw21_exposure(
    ticker_symbol: Union[str, list[str]] = None,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    根据代码、开始时间、结束时间提取基金barra申万21年行业的因子暴露

    Parameters
    ----------
    ticker_symbol : Union[str, list[str]], optional
        基金代码, 可以是字符串也可以是list, by default None
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    pd.DataFrame
        barra因子暴露表
    """
    start_date, end_date = utils.prepare_dates(start_date=start_date, end_date=end_date)
    # 提取SQL
    sql_query = f"""
    SELECT
        *
    FROM
    	fund_exposure_cne6_sw21
    WHERE
    	TRADE_DT BETWEEN '{start_date}' AND '{end_date}'
        AND r_squared >= 0.6
    """
    if ticker_symbol:
        sql_query = utils.fill_sql_ticker_symbol(
            ticker_symbol, sql_query, "TICKER_SYMBOL"
        )
    sql_query += "order by  TRADE_DT,TICKER_SYMBOL"
    return DB_CONN_JJTG_DATA.exec_query(sql_query)


def get_fund_market_barra_industries(start_date: str = None, end_date: str = None):
    """
    获取基金市场bar

    Parameters
    ----------
    start_date : str, optional
        _description_, by default None
    end_date : str, optional
        _description_, by default None

    Returns
    -------
    _type_
        _description_
    """
    print(start_date, end_date)
    main_fund_ticker = get_main_fund()
    barra_df = get_fund_barra_sw21_exposure(start_date=start_date, end_date=end_date)
    barra_df = barra_df[
        barra_df["TICKER_SYMBOL"].isin(main_fund_ticker["REL_TICKER_SYMBOL"])
    ]
    barra_df = barra_df.groupby(by=["TRADE_DT"]).mean()
    barra_df.rename(
        columns=constant_variables.BARRA_SW21_FACTOR_NAME_DICT, inplace=True
    )
    return barra_df.loc[:, "农林牧渔":"纺织服饰"]


def get_main_fund() -> list[str]:
    """
    查询主基金代码

    Returns
    -------
    list
        基金代码列表
    """
    query_sql = """
    SELECT
        TICKER_SYMBOL 
    FROM
        fund_info 
    WHERE
        IS_MAIN = 1 
        AND ESTABLISH_DATE IS NOT NULL
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["TICKER_SYMBOL"].tolist()


def get_fund_info(trade_date: str = None) -> pd.DataFrame:
    """
    获取基金信息

    Parameters
    ----------
    trade_date : str, optional
        截止到的日期, by default None

    Returns
    -------
    pd.DataFrame
        _description_
    """
    if trade_date is None:
        trade_date = datetime.datetime.now().strftime("%Y%m%d")

    query_sql = f"""
    SELECT
        TICKER_SYMBOL,
        SEC_SHORT_NAME
    FROM
        fund_info
    WHERE
        "{trade_date}" BETWEEN ESTABLISH_DATE and IFNULL(EXPIRE_DATE,"20991231")
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_tracking_idx(ticker_symbol: str, date: str = None) -> str:
    """
    获取基金跟踪指数

    Parameters
    ----------
    ticker_symbol : str
        基金代码
    date : str
        日期

    Returns
    -------
    str
        跟踪指数的代码
    """
    if date is None:
        date = datetime.datetime.now().strftime("%Y%m%d")

    query_sql = f"""
    SELECT
        IDX_SHORT_NAME 
    FROM
        fund_tracking_idx 
    WHERE 
        TICKER_SYMBOL = '{ticker_symbol}' 
        and ("{date}" between BEGIN_DATE and ifnull(END_DATE, '20991231'))
    """
    try:
        return DB_CONN_JJTG_DATA.exec_query(query_sql)["IDX_SHORT_NAME"].values[0]
    except Exception:
        return None


def get_type_own_index() -> pd.DataFrame:
    query_sql = """
    SELECT
        INDEX_CODE,
        TYPE_NAME,
        LEVEL_NUM
    FROM
        fund_type_own_index
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_inner_fund_type_median_return(
    end_date: str, level_num: int = 3
) -> pd.DataFrame:
    """
    获取内部基金分类的收益率中位数

    Parameters
    ----------
    end_date : str
        净值日期
    level_num : int, optional
        内部分类层级, by default 3

    Returns
    -------
    pd.DataFrame
        内部基金分类的收益率中位数
    """
    result_list = []

    ret_sql = f"""
    SELECT
        a.END_DATE,
        a.TICKER_SYMBOL,
        a.RETURN_RATE,
        a.RETURN_RATE_WTD,
        a.RETURN_RATE_MTD,
        a.RETURN_RATE_YTD,
        a.RETURN_RATE_1W,
        a.RETURN_RATE_1M,
        a.RETURN_RATE_3M,
        a.RETURN_RATE_6M,
        a.RETURN_RATE_1Y,
        a.RETURN_RATE_2Y,
        a.RETURN_RATE_3Y 
    FROM
        fund_nav_gr a
    WHERE
    1=1
    and END_DATE = '{end_date}'
    """
    ret_df = DB_CONN_JJTG_DATA.exec_query(ret_sql)
    for i in range(1, level_num + 1):
        query_sql = f"""
            SELECT
                a.TICKER_SYMBOL,
                c.INDEX_CODE,
                c.TYPE_NAME,
                c.LEVEL_NUM 
            FROM
                fund_type_own_temp a
                JOIN fund_type_own_index c ON c.TYPE_NAME = a.LEVEL_{i}
            WHERE
                1 = 1 
                AND c.LEVEL_NUM = {i}
                and a.REPORT_DATE = (
                    SELECT 
                    max( REPORT_DATE ) 
                    FROM fund_type_own_temp 
                    WHERE PUBLISH_DATE <= '{end_date}' 
                )
        """
        df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        df = df.merge(ret_df)

        grouped_df = (
            df.groupby(
                by=[
                    "END_DATE",
                    "INDEX_CODE",
                    "TYPE_NAME",
                    "LEVEL_NUM",
                ]
            )
            .median(True)
            .reset_index()
        )
        result_list.append(grouped_df)

    return pd.concat(result_list)


def get_inner_fund_type_avg_return(end_date: str, level_num: int = 3) -> pd.DataFrame:
    """
    获取内部基金分类的收益率均值

    Parameters
    ----------
    end_date : str
        净值日期
    level_num : int, optional
        内部分类层级, by default 3

    Returns
    -------
    pd.DataFrame
        内部基金分类的收益率中位数
    """
    result_list = []

    ret_sql = f"""
    SELECT
        a.END_DATE,
        a.TICKER_SYMBOL,
        a.RETURN_RATE,
        a.RETURN_RATE_WTD,
        a.RETURN_RATE_MTD,
        a.RETURN_RATE_YTD,
        a.RETURN_RATE_1W,
        a.RETURN_RATE_1M,
        a.RETURN_RATE_3M,
        a.RETURN_RATE_6M,
        a.RETURN_RATE_1Y,
        a.RETURN_RATE_2Y,
        a.RETURN_RATE_3Y 
    FROM
        fund_nav_gr a
    WHERE
    1=1
    and END_DATE = '{end_date}'
    """
    ret_df = DB_CONN_JJTG_DATA.exec_query(ret_sql)
    for i in range(1, level_num + 1):
        query_sql = f"""
            SELECT
                a.TICKER_SYMBOL,
                c.INDEX_CODE,
                c.TYPE_NAME,
                c.LEVEL_NUM 
            FROM
                fund_type_own_temp a
                JOIN fund_type_own_index c ON c.TYPE_NAME = a.LEVEL_{i}
            WHERE
                1 = 1 
                AND c.LEVEL_NUM = {i}
                and a.REPORT_DATE = (
                    SELECT 
                    max( REPORT_DATE ) 
                    FROM fund_type_own_temp 
                    WHERE PUBLISH_DATE <= '{end_date}' 
                )
        """
        df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        df = df.merge(ret_df)

        grouped_df = (
            df.groupby(
                by=[
                    "END_DATE",
                    "INDEX_CODE",
                    "TYPE_NAME",
                    "LEVEL_NUM",
                ]
            )
            .mean(True)
            .reset_index()
        )
        result_list.append(grouped_df)

    return pd.concat(result_list)


def get_stock_sw_industry_21(trade_dt: str = None, level_num=1) -> pd.DataFrame:
    """
    查询sw21行业

    Parameters
    ----------
    trade_dt : str, optional
        查询行业的日期, by default None

    Returns
    -------
    pd.DataFrame
        columns = [TICKER_SYMBOL, INDUSTRY_NAME]
    """
    if trade_dt is None:
        trade_dt = utils.get_now()

    query_sql = f"""
    SELECT
        TICKER_SYMBOL,
        INDUSTRIESNAME as INDUSTRY_NAME 
    FROM
        md_industry_sw21
    WHERE
        1 = 1
        and LEVELNUM = {level_num+1}
        and ENTRY_DT <= DATE('{trade_dt}')
        and IFNULL(REMOVE_DT,'2099-12-31') > DATE('{trade_dt}')
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_exposure_sector(
    ticker_symbol_list: list[str] = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    获取基金敞口暴露-板块

    Parameters
    ----------
    ticker_symbol_list : list[str], optional
        基金代码列表, by default None
    start_date : str, optional
        开始时间, by default None
    end_date : str, optional
        结束时间, by default None

    Returns
    -------
    pd.DataFrame
        基金敞口暴露-板块
    """
    query_sql = """
    SELECT
        t.TRADE_DT,
        t.TICKER_SYMBOL,
        t.cash*100 - f.cash AS '现金:组合-市场',
        t.LARGE_GROWTH*100  - f.LARGE_GROWTH AS '大盘成长:组合-市场',
        t.MID_GROWTH*100  - f.MID_GROWTH AS '中盘成长:组合-市场',
        t.SMALL_GROWTH*100  - f.SMALL_GROWTH AS '小盘成长:组合-市场',
        t.LARGE_VALUE*100  - f.LARGE_VALUE AS '大盘价值:组合-市场',
        t.MID_VALUE*100  - f.MID_VALUE AS '中盘价值:组合-市场',
        t.SMALL_VALUE*100  - f.SMALL_VALUE AS '小盘价值:组合-市场',
        (t.LARGE_GROWTH + t.MID_GROWTH + t.SMALL_GROWTH)*100  - ( f.LARGE_GROWTH + f.MID_GROWTH + f.SMALL_GROWTH ) AS '成长:组合-市场',
        (t.LARGE_VALUE + t.MID_VALUE + t.SMALL_VALUE)*100  - ( f.LARGE_VALUE + f.MID_VALUE + f.SMALL_VALUE ) AS '价值:组合-市场',
        (t.LARGE_VALUE + t.LARGE_GROWTH)*100  - ( f.LARGE_VALUE + f.LARGE_GROWTH ) AS '大市值:组合-市场',
        (t.MID_VALUE + t.MID_GROWTH)*100  - ( f.MID_VALUE + f.MID_GROWTH ) AS '中市值:组合-市场',
        (t.SMALL_VALUE + t.SMALL_GROWTH)*100  - ( f.SMALL_VALUE + f.SMALL_GROWTH ) AS '小市值:组合-市场',
        (t.LARGE_GROWTH + t.MID_GROWTH + t.SMALL_GROWTH)*100  AS '成长:组合',
        f.LARGE_GROWTH + f.MID_GROWTH + f.SMALL_GROWTH AS '成长:市场',
        (t.LARGE_VALUE + t.MID_VALUE + t.SMALL_VALUE)*100  AS '价值:组合',
        f.LARGE_VALUE + f.MID_VALUE + f.SMALL_VALUE AS '价值:市场',
        (t.LARGE_VALUE + t.LARGE_GROWTH)*100  AS '大市值:组合',
        f.LARGE_VALUE + f.LARGE_GROWTH AS '大市值:市场',
        (t.MID_VALUE + t.MID_GROWTH)*100  AS '中市值:组合',
        f.MID_VALUE + f.MID_GROWTH AS '中市值:市场',
        (t.SMALL_VALUE + t.SMALL_GROWTH)*100  AS '小市值:组合',
        f.SMALL_VALUE + f.SMALL_GROWTH AS '小市值:市场' 
    FROM
        fund_style_stock t
        JOIN view_monitor_fund_market_style f ON t.trade_dt = f.trade_dt 
    WHERE
        1 = 1 
    """
    if ticker_symbol_list:
        query_sql += f"""
            and t.TICKER_SYMBOL in ({utils.convert_list_to_str(ticker_symbol_list)})
        """
    if start_date:
        query_sql += f"""
            and t.TRADE_DT >= '{start_date}'
        """
    if end_date:
        query_sql += f"""
            and t.TRADE_DT <= '{end_date}'
        """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_bond_holdings(report_date: str) -> pd.DataFrame:
    """
    获取基金券种持仓类别

    Parameters
    ----------
    report_date : str
        报告期.

    Returns
    -------
    DataFrame
        基金的持仓券种.
    """

    query_sql = f"""
    WITH a AS (
        SELECT
            REPORT_DATE,
            TICKER_SYMBOL,
            BOND_RATIO_IN_NA,
            F_PRT_COVERTBONDTONAV,
            IFNULL( F_PRT_GOVBONDTONAV, 0 ) + IFNULL( F_PRT_LOCALGOVBONDTONAV, 0 ) + IFNULL( F_PRT_CTRBANKBILLTONAV, 0 ) + IFNULL( F_PRT_POLIFINANBDTONAV, 0 ) AS INTEREST_BOND 
        FROM
            fund_asset_own 
        WHERE
            REPORT_DATE = '{report_date}' 
        ),
        b AS ( SELECT a.*, BOND_RATIO_IN_NA - F_PRT_COVERTBONDTONAV - INTEREST_BOND AS CREDIT_BOND FROM a ) SELECT
        REPORT_DATE,
        TICKER_SYMBOL,
        BOND_RATIO_IN_NA,
        INTEREST_BOND,
        CREDIT_BOND,
        ( CREDIT_BOND + 0.0001 )/(
            INTEREST_BOND + 0.0001 
        ) AS `CREDIT_TO_INTEREST` 
    FROM
        b
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_bond_holdings_period_mean(date: str, period_num: int = 4) -> pd.DataFrame:
    """
    获取指定date前n期的基金券种持仓类别

    Parameters
    ----------
    date : str
        日期.
    period_num : int, optional
        滚动的期数. The default is 4.

    Returns
    -------
    fund_bond_holdings : DataFrame
        基金券种持仓类别.
    """

    # 滚动4期的报告期
    report_dates_list = utils.get_report_date(date, period_num)

    list_risk_exposure = []
    for report_date in report_dates_list:
        temp_df = get_fund_bond_holdings(report_date)
        list_risk_exposure.append(temp_df)

    fund_bond_holdings = pd.concat(list_risk_exposure)

    return (
        fund_bond_holdings.groupby(by=["TICKER_SYMBOL"])[
            [
                "BOND_RATIO_IN_NA",
                "INTEREST_BOND",
                "CREDIT_BOND",
                "CREDIT_TO_INTEREST",
            ]
        ]
        .mean()
        .reset_index()
    )


def get_fund_asset_own(
    report_date: str = None, ticker_symbol: str = None
) -> pd.DataFrame:
    """
    获取基金资产表

    Parameters
    ----------
    report_date : str, optional
        报告期, by default None
    ticker_symbol : str, optional
        基金代码, by default None

    Returns
    -------
    pd.DataFrame
        基金资产表
    """
    query_sql = """
        select 
            * 
        from 
            fund_asset_own 
        where 
            1 = 1
    """
    if report_date is not None:
        query_sql += f" and report_date = '{report_date}'"
    if ticker_symbol is not None:
        query_sql += f" and ticker_symbol = '{ticker_symbol}'"
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_alpha_performance(
    trade_dt: str,
    indicator: str,
    cycle: str,
    level_num=3,
    n_groups=10,
    if_desc=1,
    limit_net_asset=1,
) -> pd.DataFrame:
    """
    获取基金超额收益表现

    Parameters
    ----------
    trade_dt : str
        交易日起
    indicator : str
        指标
    cycle : str
        周期
    level_num : int, optional
        分类层级, by default 3
    n_groups : int, optional
        分组数, by default 10
    if_desc : int, optional
        是否按照得分从大到小排列, by default 1
    limit_net_asset : int, optional
        最近一期最小规模单位亿元, by default 1
    Returns
    -------
    pd.DataFrame
        基金超额收益表现
    """
    order_dict = {
        0: "ASC",
        1: "DESC",
    }
    patition_str = ",".join([f"b.LEVEL_{i}" for i in range(1, level_num + 1)])
    query_sql = f"""
    WITH a AS (
        SELECT
            END_DATE,
            TICKER_SYMBOL,
            `LEVEL`,
            sum( CASE INDICATOR WHEN '{indicator}' THEN {cycle} ELSE 0 END ) AS {indicator}_{cycle}
        FROM
            fund_derivatives_fund_alpha_performance 
        WHERE
            1 = 1 
            AND END_DATE = '{trade_dt}' 
            AND {cycle} IS NOT NULL 
        GROUP BY
            END_DATE,
            TICKER_SYMBOL,
            `LEVEL` 
        ) SELECT
        a.END_DATE,
        a.TICKER_SYMBOL,
        a.LEVEL,
        b.LEVEL_1,
        b.LEVEL_2,
        b.LEVEL_3,
        c.IS_MAIN,
        c.IS_ILLIQUID,
        PERCENT_RANK() over ( PARTITION BY END_DATE, a.LEVEL, {patition_str} ORDER BY {indicator}_{cycle} )* 100 AS {indicator}_{cycle}_SCORE,
        NTILE( {n_groups} ) over ( PARTITION BY END_DATE, a.LEVEL, {patition_str} ORDER BY {indicator}_{cycle} {order_dict[if_desc]} ) AS {indicator}_{cycle}_GROUP 
    FROM
        a
        LEFT JOIN fund_type_own b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL
        LEFT JOIN fund_info c ON c.TICKER_SYMBOL = b.TICKER_SYMBOL 
        LEFT JOIN fund_asset_own d ON d.TICKER_SYMBOL = b.TICKER_SYMBOL 
	    AND d.report_date = b.report_date 
    WHERE
        1 = 1 
        AND b.PUBLISH_DATE = ( SELECT max( PUBLISH_DATE ) FROM fund_type_own WHERE PUBLISH_DATE <= '{trade_dt}' ) 
        AND a.`LEVEL` = 'LEVEL_{level_num}' 
        AND ifnull( c.EXPIRE_DATE, "2099-12-31" ) >= '{trade_dt}' 
        AND c.ESTABLISH_DATE <= '{trade_dt}'
        AND d.NET_ASSET >= {limit_net_asset} * POWER(10,8)
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_jy_fund_type(date: str = None) -> pd.DataFrame:
    """
    获取聚源分类基金类型

    Parameters
    ----------
    date : str, optional
        日期, by default None

    Returns
    -------
    pd.DataFrame
        columns = [
            TICKER_SYMBOL,
            S_INFO_SECTORENTRYDT,
            S_INFO_SECTOREXITDT,
            INDUSTRIESNAME_1,
            INDUSTRIESNAME_2,
            INDUSTRIESNAME_3
        ]
    """
    if date is None:
        date = utils.get_now()

    query_sql = f"""
        SELECT
            TICKER_SYMBOL,
            S_INFO_SECTORENTRYDT,
            S_INFO_SECTOREXITDT,
            INDUSTRIESNAME_1,
            INDUSTRIESNAME_2,
            INDUSTRIESNAME_3
        FROM
            fund_type_jy
        WHERE
            1 = 1
            and {date} between S_INFO_SECTORENTRYDT 
            and ifnull(S_INFO_SECTOREXITDT, '20991231')
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_score(trade_dt: str) -> pd.DataFrame:
    """
    获取基金评分

    Parameters
    ----------
    trade_dt : str
        交易日

    Returns
    -------
    pd.DataFrame
        结果表,子项得分并没有乘以权重，满分为100分
    """
    query_sql = f"""
    WITH fof AS (
        SELECT
            a.TICKER_SYMBOL,
            b.SEC_SHORT_NAME,
            a.LEVEL,
            b.LEVEL_1,
            b.LEVEL_2,
        CASE
            WHEN EQUITY_PORT <= 5 THEN
            '00-05' 
            WHEN EQUITY_PORT <= 15 THEN
            '05-15' 
            WHEN EQUITY_PORT <= 25 THEN
            '15-25' 
            WHEN EQUITY_PORT <= 45 THEN
            "25-45" 
            WHEN EQUITY_PORT <= 65 THEN
            "45-65" ELSE '65-100' 
            END AS LEVEL_3,
            F_AVGRETURN_YEAR,
            F_CALMAR_YEAR,
            F_SHARPRATIO_YEAR,
            F_MAXDOWNSIDE_YEAR,
            F_AVGRETURN_THREEYEAR,
            F_CALMAR_THREEYEAR,
            F_SHARPRATIO_THREEYEAR,
            F_MAXDOWNSIDE_THREEYEAR 
        FROM
            fund_performance_rank_pct a
            JOIN fund_type_own b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL
            JOIN fof_type c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND a.TRADE_DT = '{trade_dt}' 
            AND b.REPORT_DATE = ( SELECT max( REPORT_DATE ) FROM fund_type_own WHERE PUBLISH_DATE <= '{trade_dt}' ) 
            AND a.LEVEL = 'LEVEL_1' 
            AND b.level_1 = "FOF" 
        ),
        level_3 AS (
        SELECT
            TICKER_SYMBOL,
            SEC_SHORT_NAME,
            'LEVEL_3' AS LEVEL,
            LEVEL_1,
            LEVEL_2,
            LEVEL_3,
        CASE
            WHEN F_AVGRETURN_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_AVGRETURN_YEAR ASC )* 100 
            END AS F_AVGRETURN_YEAR,
        CASE
            WHEN F_CALMAR_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_CALMAR_YEAR ASC )* 100 
            END AS F_CALMAR_YEAR,
        CASE
            WHEN F_SHARPRATIO_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_SHARPRATIO_YEAR ASC )* 100 
            END AS F_SHARPRATIO_YEAR,
        CASE
            WHEN F_MAXDOWNSIDE_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_MAXDOWNSIDE_YEAR ASC )* 100 
            END AS F_MAXDOWNSIDE_YEAR,
        CASE
            WHEN F_AVGRETURN_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_AVGRETURN_THREEYEAR ASC )* 100 
            END AS F_AVGRETURN_THREEYEAR,
        CASE
            WHEN F_CALMAR_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_CALMAR_THREEYEAR ASC )* 100 
            END AS F_CALMAR_THREEYEAR,
        CASE
            WHEN F_SHARPRATIO_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_SHARPRATIO_THREEYEAR ASC )* 100 
            END AS F_SHARPRATIO_THREEYEAR,
        CASE

            WHEN F_MAXDOWNSIDE_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_MAXDOWNSIDE_THREEYEAR ASC )* 100 
            END AS F_MAXDOWNSIDE_THREEYEAR 
        FROM
            fof
        ),
        level_2 AS (
        SELECT
            TICKER_SYMBOL,
            SEC_SHORT_NAME,
            'LEVEL_2' AS LEVEL,
            LEVEL_1,
            LEVEL_2,
            LEVEL_3,
        CASE
            WHEN F_AVGRETURN_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_2 ORDER BY F_AVGRETURN_YEAR ASC )* 100 
            END AS F_AVGRETURN_YEAR,
        CASE
            WHEN F_CALMAR_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_2 ORDER BY F_CALMAR_YEAR ASC )* 100 
            END AS F_CALMAR_YEAR,
        CASE
            WHEN F_SHARPRATIO_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_2 ORDER BY F_SHARPRATIO_YEAR ASC )* 100 
            END AS F_SHARPRATIO_YEAR,
        CASE
            WHEN F_MAXDOWNSIDE_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_2 ORDER BY F_MAXDOWNSIDE_YEAR ASC )* 100 
            END AS F_MAXDOWNSIDE_YEAR,
        CASE
            WHEN F_AVGRETURN_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_2 ORDER BY F_AVGRETURN_THREEYEAR ASC )* 100 
            END AS F_AVGRETURN_THREEYEAR,
        CASE
            WHEN F_CALMAR_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_2 ORDER BY F_CALMAR_THREEYEAR ASC )* 100 
            END AS F_CALMAR_THREEYEAR,
        CASE
            WHEN F_SHARPRATIO_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_SHARPRATIO_THREEYEAR ASC )* 100 
            END AS F_SHARPRATIO_THREEYEAR,
        CASE
            WHEN F_MAXDOWNSIDE_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_2 ORDER BY F_MAXDOWNSIDE_THREEYEAR ASC )* 100 
            END AS F_MAXDOWNSIDE_THREEYEAR 
        FROM
            fof 
        ),
        level_1 AS (
        SELECT
            TICKER_SYMBOL,
            SEC_SHORT_NAME,
            'LEVEL_1' AS LEVEL,
            LEVEL_1,
            LEVEL_2,
            LEVEL_3,
        CASE
            WHEN F_AVGRETURN_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY level_1 ORDER BY F_AVGRETURN_YEAR ASC )* 100 
            END AS F_AVGRETURN_YEAR,
        CASE
            WHEN F_CALMAR_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY level_1 ORDER BY F_CALMAR_YEAR ASC )* 100 
            END AS F_CALMAR_YEAR,
        CASE
            WHEN F_SHARPRATIO_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY level_1 ORDER BY F_SHARPRATIO_YEAR ASC )* 100 
            END AS F_SHARPRATIO_YEAR,
        CASE
            WHEN F_MAXDOWNSIDE_YEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY level_1 ORDER BY F_MAXDOWNSIDE_YEAR ASC )* 100 
            END AS F_MAXDOWNSIDE_YEAR,
        CASE
            WHEN F_AVGRETURN_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY level_1 ORDER BY F_AVGRETURN_THREEYEAR ASC )* 100 
            END AS F_AVGRETURN_THREEYEAR,
        CASE
            WHEN F_CALMAR_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY level_1 ORDER BY F_CALMAR_THREEYEAR ASC )* 100 
            END AS F_CALMAR_THREEYEAR,
        CASE
            WHEN F_SHARPRATIO_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY LEVEL_3 ORDER BY F_SHARPRATIO_THREEYEAR ASC )* 100 
            END AS F_SHARPRATIO_THREEYEAR,
        CASE
            WHEN F_MAXDOWNSIDE_THREEYEAR IS NULL THEN
            NULL ELSE PERCENT_RANK() over ( PARTITION BY level_1 ORDER BY F_MAXDOWNSIDE_THREEYEAR ASC )* 100 
            END AS F_MAXDOWNSIDE_THREEYEAR 
        FROM
            fof 
        ),
        temp_perf_rank AS (
        SELECT
            * 
        FROM
            level_1 UNION
        SELECT
            * 
        FROM
            level_2 UNION
        SELECT
            * 
        FROM
            level_3 UNION
        SELECT
            a.TICKER_SYMBOL,
            b.SEC_SHORT_NAME,
            a.LEVEL,
            b.LEVEL_1,
            b.LEVEL_2,
            b.LEVEL_3,
            F_AVGRETURN_YEAR,
            F_CALMAR_YEAR,
            F_SHARPRATIO_YEAR,
            F_MAXDOWNSIDE_YEAR,
            F_AVGRETURN_THREEYEAR,
            F_CALMAR_THREEYEAR,
            F_SHARPRATIO_THREEYEAR,
            F_MAXDOWNSIDE_THREEYEAR 
        FROM
            fund_performance_rank_pct a
            JOIN fund_type_own b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND a.TRADE_DT = '{trade_dt}'
            AND b.REPORT_DATE = ( SELECT max( REPORT_DATE ) FROM fund_type_own WHERE PUBLISH_DATE <= '{trade_dt}' ) 
            AND b.level_1 != "FOF" 
        ),
        a AS (
        SELECT
            TICKER_SYMBOL,
            SEC_SHORT_NAME,
            LEVEL,
            LEVEL_1,
            LEVEL_2,
            LEVEL_3,
            ROUND(( 100-a.F_AVGRETURN_YEAR ), 2 ) AS F_AVGRETURN_YEAR,
            ROUND( 100-a.F_CALMAR_YEAR, 2 ) AS F_CALMAR_YEAR,
            ROUND(( 100-a.F_SHARPRATIO_YEAR ), 2 ) AS F_SHARPRATIO_YEAR,
            ROUND(( 100-a.F_MAXDOWNSIDE_YEAR ), 2 ) AS F_MAXDOWNSIDE_YEAR,
            ROUND( 100-ifnull ( a.F_AVGRETURN_THREEYEAR, 50 ), 2 ) AS F_AVGRETURN_THREEYEAR,
            ROUND( 100-ifnull ( a.F_CALMAR_THREEYEAR, 50 ), 2 ) AS F_CALMAR_THREEYEAR,
            ROUND( 100-ifnull ( a.F_SHARPRATIO_THREEYEAR, 50 ), 2 ) AS F_SHARPRATIO_THREEYEAR,
            ROUND( 100-ifnull ( a.F_MAXDOWNSIDE_THREEYEAR, 50 ), 2 ) AS F_MAXDOWNSIDE_THREEYEAR 
        FROM
            temp_perf_rank a 
        WHERE
            1 = 1 
            AND a.F_AVGRETURN_YEAR IS NOT NULL 
        ),
        b AS (
        SELECT
            LEVEL_1,
            sum( CASE INDICATOR WHEN '累计收益率' THEN WEIGHT ELSE 0 END ) AS `累计收益率`,
            sum( CASE INDICATOR WHEN '年化收益回撤比' THEN WEIGHT ELSE 0 END ) AS `年化收益回撤比`,
            sum( CASE INDICATOR WHEN '收益波动比' THEN WEIGHT ELSE 0 END ) AS `收益波动比`,
            sum( CASE INDICATOR WHEN '最大回撤' THEN WEIGHT ELSE 0 END ) AS `最大回撤` 
        FROM
            portfolio_evaluation 
        GROUP BY
            LEVEL_1 
        ),
        c AS (
        SELECT
            a.ticker_symbol as '代码',
            a.SEC_SHORT_NAME as '简称',
            a.LEVEL as '评分类别',
            a.LEVEL_1 as '1级分类',
            a.LEVEL_2 as '2级分类',
            a.LEVEL_3 as '3级分类',
            round( ( F_AVGRETURN_YEAR + F_AVGRETURN_THREEYEAR )/ 2, 2 ) AS `累计收益率`,
            round( ( F_CALMAR_YEAR + F_CALMAR_THREEYEAR )/ 2, 2 ) AS `年化收益回撤比`,
            round( ( F_SHARPRATIO_YEAR + F_SHARPRATIO_YEAR )/ 2, 2 ) AS `收益波动比`,
            round( ( F_MAXDOWNSIDE_YEAR + F_MAXDOWNSIDE_THREEYEAR )/ 2, 2 ) AS `最大回撤` 
        FROM
            a 
        ) SELECT
        c.*,
        round(
            c.`累计收益率` * b.`累计收益率` + c.`最大回撤` * b.`最大回撤` + ifnull( c.`年化收益回撤比` * b.`年化收益回撤比`, 0 ) + c.`收益波动比` * b.`收益波动比`,
            2 
        ) AS '总分' 
        FROM
        c
        JOIN b ON b.level_1 = c.`1级分类` 
        WHERE
        1 = 1
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


__all__ = [
    "get_fund_risk_exposure",
    "get_risk_exposure_period_mean",
    "get_sector_exposure",
    "get_sector_exposure_period_mean",
    "get_hk_exposure",
    "get_hk_exposure_period_mean",
    "get_fund_stocks_holding",
    "get_fund_adj_nav",
    "get_fund_nav_growth",
    "get_fund_redeem_fee",
    "get_own_fund_type",
    "get_fund_index",
    "get_fund_barra_sw21_exposure",
    "get_fund_market_barra_industries",
    "get_main_fund",
    "get_fund_info",
    "get_fund_tracking_idx",
    "get_type_own_index",
    "get_inner_fund_type_median_return",
    "get_inner_fund_type_avg_return",
    "get_stock_sw_industry_21",
    "get_fund_exposure_sector",
    "get_fund_bond_holdings",
    "get_fund_bond_holdings_period_mean",
    "get_fund_asset_own",
    "get_fund_alpha_performance",
    "get_jy_fund_type",
    "get_fund_score",
]
