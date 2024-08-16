# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 14:03:48 2022

@author: Wilcoxon
"""

import logging
from functools import reduce

import pandas as pd

import data_functions.fund_data as fd
import quant_utils.data_moudle as dm
from fund_db.fund_db_cal_func import cal_enhanced_index_performance
from quant_utils.constant import LOG_FILE_PATH, LOG_FORMAT
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_DATAYES, DB_CONN_JJTG_DATA, DB_CONN_JY
from quant_utils.send_email import MailSender

logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format=LOG_FORMAT,
)


def get_max_jsid(table: str, id_str: str = "UKID"):
    query_sql = f"""
    select ifnull(max({id_str}),0) as jsid from {table}
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["jsid"].values[0]


def query_from_remote_upsert_into_local(
    query_sql: str,
    table: str,
    query_db_conn,
    upsert_db_conn=DB_CONN_JJTG_DATA,
    if_drop_columns: bool = True,
):
    """
    从远程查询，插入本地数据表

    Parameters
    ----------
    query_sql : str
        查询语句
    table : str
        需要写入的表
    query_db_conn : _type_
        查询的数据库联接

    upsert_db_conn : _type_
        upsert的数据库联接
    """
    # 查询数据
    df = query_db_conn.exec_query(query_sql)
    print(f"{table}查询结束!")
    drop_cloumns = ["OBJECT_ID", "SEC_ID", "OPMODE", "S_INFO_CODE", "JSID"]
    if if_drop_columns:
        if columns_needed_to_drop := list(set(df.columns) & set(drop_cloumns)):
            df.drop(columns=columns_needed_to_drop, inplace=True)

    df.rename(
        columns={
            # "S_INFO_CODE": "TICKER_SYMBOL",
            "TRADE_DATE": "TRADE_DT",
            "OPDATE": "UPDATE_TIME",
            "update_time": "UPDATE_TIME",
            "EndDate": "TRADE_DT",
            "InsertTime": "CREATE_TIME",
            "UpdateTime": "UPDATE_TIME",
        },
        inplace=True,
    )
    # 写入数据
    df = df.drop_duplicates()
    if not df.empty:
        upsert_db_conn.upsert(df_to_upsert=df, table=table)
    else:
        print(f"{table}-无数据插入!")


def update_fund_performance_attribution_campisi(update_date: str = None):
    """
    更新Campisi债券型基金业绩归因

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        DATE_FORMAT(a.EndDate,"%Y-%m-%d") as TRADE_DT, 
        b.SecuCode as TICKER_SYMBOL,
        a.IndexCycle,
        a.DurationMagtContri,
        a.SpreadContri,
        a.CouponContri,
        a.InsertTime as CREATE_TIME,
        a.UpdateTime as UPDATE_TIME
    FROM
        MF_CampisiPerfAtrb a
        JOIN secumain b ON b.InnerCode = a.InnerCode
    where a.EndDate >= '{update_date}'
    """
    # query_from_jy_upsert_into_local_db(
    #     query_sql, table="fund_performance_attribution_campisi"
    # )
    query_from_remote_upsert_into_local(
        query_sql,
        table="fund_performance_attribution_campisi",
        query_db_conn=DB_CONN_JY,
    )


def update_fund_performance_attribution_carhart(update_date: str = None):
    """
    基金Carhart四因子模型业绩归因

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        b.SecuCode as TICKER_SYMBOL,
        DATE_FORMAT(a.EndDate,"%Y-%m-%d") as TRADE_DT, 
        a.IndexCycle,
        a.MktCoeffi,
        a.MktPValue,
        a.SMBCoeffi,
        a.SMBPVal,
        a.HMLCoeffi,
        a.HMLPVal,
        a.MOMCoeffi,
        a.MOMPVal,
        a.Alpha,
        a.AlphaPVal,
        a.AdjRSquare,
        a.InsertTime as CREATE_TIME,
        a.UpdateTime as UPDATE_TIME
    FROM
        MF_FundCarhartPerfAtrb a
        JOIN secumain b ON b.InnerCode = a.InnerCode
    where a.EndDate >= "{update_date}"
    """
    # query_from_jy_upsert_into_local_db(
    #     query_sql, table="fund_performance_attribution_carhart"
    # )
    query_from_remote_upsert_into_local(
        query_sql,
        table="fund_performance_attribution_carhart",
        query_db_conn=DB_CONN_JY,
    )


def update_fund_adj_nav(update_date: str = None):
    """
    更新基金复权净值

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT

    # query_sql = f"""
    # SELECT
    #     SUBSTRING_INDEX( F_INFO_WINDCODE, '.', 1 ) AS TICKER_SYMBOL,
    #     PRICE_DATE AS END_DATE,
    #     F_NAV_UNIT AS UNIT_NAV,
    #     F_NAV_ADJUSTED AS ADJ_NAV,
    #     F_NAV_ADJFACTOR AS ADJ_FACTOR
    # FROM
    #     chinamutualfundnav
    # WHERE
    #     1 = 1
    #     AND PRICE_DATE = "{update_date}"
    # """

    # query_from_remote_upsert_into_local(
    #     query_sql, table="fund_adj_nav", query_db_conn=DB_CONN_JJTG_DATA
    # )

    query_sql = f"""
    SELECT
        a.TICKER_SYMBOL,
        a.END_DATE,
        ( a.ADJ_NAV / c.ADJ_NAV - 1 )* 100 AS RETURN_RATE_TO_PREV_DT
    FROM
        fund_adj_nav a
        JOIN md_tradingdaynew b ON b.TRADE_DT = a.END_DATE 
        AND b.SECU_MARKET = 83
        JOIN fund_adj_nav c ON c.END_DATE = b.PREV_TRADE_DATE 
        AND c.TICKER_SYMBOL = a.TICKER_SYMBOL 
    WHERE
        1 = 1 
        AND a.UPDATE_TIME >= '{update_date}'
    """

    query_from_remote_upsert_into_local(
        query_sql, table="fund_adj_nav", query_db_conn=DB_CONN_JJTG_DATA
    )


def update_fund_index_description(update_date: str = None):
    """
    更新基金指数的描述表

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """

    if update_date is None:
        update_date = LAST_TRADE_DT

    query_sql = """
    SELECT DISTINCT
        TICKER_SYMBOL,
        S_INFO_WINDCODE,
        SUBSTRING_INDEX( S_INFO_WINDCODE, ".",- 1 ) AS SUFFIX,
        S_INFO_NAME,
        min( TRADE_DT ) AS BASE_DATE,
        max(TRADE_DT ) as LAST_DATE
    FROM
        fund_index_eod 
    GROUP BY
        TICKER_SYMBOL,
        S_INFO_WINDCODE,
        S_INFO_NAME
    """
    query_from_remote_upsert_into_local(
        query_sql, table="fund_index_description", query_db_conn=DB_CONN_JJTG_DATA
    )


def update_fund_deriavetes_inner_fund_ret(update_date: str = None):
    """
    更新知己内部分类指数收益

    Parameters
    ----------
    update_date : str, optional
       更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    result = dm.get_inner_fund_type_median_return(end_date=update_date)
    DB_CONN_JJTG_DATA.upsert(result, table="fund_derivatives_inner_fund_ret")
    avg = dm.get_inner_fund_type_avg_return(end_date=update_date)
    DB_CONN_JJTG_DATA.upsert(avg, table="fund_derivatives_inner_fund_ret_avg")


def update_fund_derivatives_fund_log_alpha(update_date: str = None):
    """
    更新基金相对基准的超额收益

    Parameters
    ----------
    update_date : str, optional
       更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    WITH a AS (
    SELECT
            b.END_DATE,
            b.TICKER_SYMBOL,
            a.LEVEL_1,
            a.LEVEL_2,
            a.LEVEL_3,
            b.LOG_RET - log( 1 + c.RETURN_RATE / 100 )* 100 AS LOG_ALPHA_LEVEL_1,
            b.LOG_RET - log( 1 + d.RETURN_RATE / 100 )* 100 AS LOG_ALPHA_LEVEL_2,
            b.LOG_RET - log( 1 + e.RETURN_RATE / 100 )* 100 AS LOG_ALPHA_LEVEL_3 
        FROM
            fund_type_own_temp a
            JOIN fund_adj_nav b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL
            JOIN fund_derivatives_inner_fund_ret c ON c.END_DATE = b.END_DATE 
            AND c.TYPE_NAME = a.LEVEL_1 
            JOIN fund_derivatives_inner_fund_ret d ON d.END_DATE = b.END_DATE 
            AND d.TYPE_NAME = a.LEVEL_2 
                JOIN fund_derivatives_inner_fund_ret e ON e.END_DATE = b.END_DATE 
            AND e.TYPE_NAME = a.LEVEL_3 
        WHERE
            1 = 1 
            AND b.END_DATE = '{update_date}' 
            and a.REPORT_DATE = ( SELECT max( REPORT_DATE ) FROM fund_type_own_temp WHERE PUBLISH_DATE <= '{update_date}' )
        ) SELECT
        a.*,
        a.LOG_ALPHA_LEVEL_1 + IFNULL( c.CUM_LOG_ALPHA_LEVEL_1, 0 ) AS CUM_LOG_ALPHA_LEVEL_1,
        a.LOG_ALPHA_LEVEL_2 + IFNULL( c.CUM_LOG_ALPHA_LEVEL_2, 0 ) AS CUM_LOG_ALPHA_LEVEL_2,
        a.LOG_ALPHA_LEVEL_3 + IFNULL( c.CUM_LOG_ALPHA_LEVEL_3, 0 ) AS CUM_LOG_ALPHA_LEVEL_3,
        exp(( a.LOG_ALPHA_LEVEL_1 + IFNULL( c.CUM_LOG_ALPHA_LEVEL_1, 0 )) / 100 ) AS ALPHA_NAV_LEVEL_1,
        exp(( a.LOG_ALPHA_LEVEL_2 + IFNULL( c.CUM_LOG_ALPHA_LEVEL_2, 0 )) / 100 ) AS ALPHA_NAV_LEVEL_2,
        exp(( a.LOG_ALPHA_LEVEL_3 + IFNULL( c.CUM_LOG_ALPHA_LEVEL_2, 0 )) / 100 ) AS ALPHA_NAV_LEVEL_3 
    FROM
        a
        LEFT JOIN md_tradingdaynew b ON a.END_DATE = b.TRADE_DT 
        AND b.SECU_MARKET = 83
        LEFT JOIN fund_derivatives_fund_log_alpha c ON c.END_DATE = b.PREV_TRADE_DATE 
        AND c.TICKER_SYMBOL = a.TICKER_SYMBOL
    """
    query_from_remote_upsert_into_local(
        query_sql,
        table="fund_derivatives_fund_log_alpha",
        query_db_conn=DB_CONN_JJTG_DATA,
    )

    query_sql = f"""
    SELECT 
        END_DATE, TICKER_SYMBOL, ALPHA_NAV_LEVEL_1, ALPHA_NAV_LEVEL_2, 
        ALPHA_NAV_LEVEL_3 
    FROM 
        fund_derivatives_fund_log_alpha 
    WHERE END_DATE = '{update_date}' 
    """
    query_from_remote_upsert_into_local(
        query_sql,
        table="fund_derivatives_fund_alpha",
        query_db_conn=DB_CONN_JJTG_DATA,
    )


def update_barra_data(update_date: str = None):
    """
    更新barra风险模型数据

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    tables = [
        "dy1d_covariance_cne6",
        "dy1d_covariance_cne6_sw21",
        "dy1d_exposure_cne6",
        "dy1d_exposure_cne6_sw21",
        "dy1d_factor_ret_cne6",
        "dy1d_factor_ret_cne6_sw21",
        "dy1d_specific_ret_cne6",
        "dy1d_specific_ret_cne6_sw21",
        "dy1d_srisk_cne6",
        "dy1d_srisk_cne6_sw21",
        "dy1l_covariance_cne6",
        "dy1l_covariance_cne6_sw21",
        "dy1l_srisk_cne6",
        "dy1l_srisk_cne6_sw21",
        "dy1s_covariance_cne6",
        "dy1s_covariance_cne6_sw21",
        "dy1s_exposure_cne6",
        "dy1s_exposure_cne6_sw21",
        "dy1s_factor_ret_cne6",
        "dy1s_factor_ret_cne6_sw21",
        "dy1s_specific_ret_cne6",
        "dy1s_specific_ret_cne6_sw21",
        "dy1s_srisk_cne6",
        "dy1s_srisk_cne6_sw21",
    ]
    for table in tables:
        query_sql = f"""
        select 
            * 
        from 
            {table} 
        where 
            TRADE_DATE >= {update_date}
        """
        query_from_remote_upsert_into_local(
            query_sql=query_sql,
            table=table,
            query_db_conn=DB_CONN_DATAYES,
        )


def update_fund_performance_factor_ret(update_date: str = None):
    """
    更新基金归因因子收益率，频率为周度

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
        select
            *
        from
            MF_PerfAtrbFactors
        where
            EndDate >= '{update_date}'
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table="fund_performance_factor_ret",
        query_db_conn=DB_CONN_JY,
    )


def update_fund_derivatives_enhanced_index_alpha(update_date: str = None):
    """
    更新指数增强基金超额收益

    Parameters
    ----------
    update_date : str, optional
        _description_, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    WITH a AS (
        SELECT
            a.END_DATE,
            a.TICKER_SYMBOL,
            b.SEC_SHORT_NAME,
            c.IDX_SHORT_NAME,
            a.LOG_RET AS FUND_LOG_RET,
            (log(1+ChangePCT/100)*100) AS IDX_LOG_RET,
            a.LOG_RET - (log(1+ChangePCT/100)*100) AS LOG_ALPHA 
        FROM
            fund_adj_nav a
            JOIN fund_type_own b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL
            JOIN fund_tracking_idx c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL
            JOIN jy_indexquote d ON d.SecuCode = c.IDX_SYMBOL 
            AND d.TradingDay = a.END_DATE 
        WHERE
            b.LEVEL_2 = '指数增强' 
            AND a.END_DATE = DATE ( "{update_date}" ) 
            AND c.BEGIN_DATE <= DATE ( "{update_date}" ) AND ifnull( c.END_DATE, '2099-12-31' ) > DATE ( "{update_date}" ) 
        AND b.REPORT_DATE = ( SELECT max( REPORT_DATE ) FROM fund_type_own WHERE PUBLISH_DATE <= DATE ( "{update_date}" ) )) SELECT
        a.*,
        a.LOG_ALPHA + ifnull( c.CUM_LOG_ALPHA, 0 ) AS CUM_LOG_ALPHA,
        exp(( a.LOG_ALPHA + ifnull( c.CUM_LOG_ALPHA, 0 ))/ 100 ) AS CUM_ALPHA_NAV 
    FROM
        a
        LEFT JOIN md_tradingdaynew b ON a.END_DATE = b.TRADE_DT 
        AND b.SECU_MARKET = 83
        LEFT JOIN fund_derivatives_enhanced_index_alpha c ON c.END_DATE = b.PREV_TRADE_DATE 
        AND c.TICKER_SYMBOL = a.TICKER_SYMBOL
    """
    query_from_remote_upsert_into_local(
        query_sql,
        table="fund_derivatives_enhanced_index_alpha",
        query_db_conn=DB_CONN_JJTG_DATA,
    )

    query_sql = f"""
        with a as (SELECT
            a.END_DATE,
            a.TICKER_SYMBOL,
            b.SEC_SHORT_NAME,
            c.IDX_SHORT_NAME,
            a.LOG_RET AS FUND_LOG_RET,
            (log(1+ChangePCT/100)*100) AS IDX_LOG_RET,
            a.LOG_RET - (log(1+ChangePCT/100)*100) AS LOG_ALPHA 
        FROM
            fund_adj_nav a
            JOIN fund_type_own b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL
            JOIN fund_tracking_idx c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL
            JOIN jy_indexquote d ON d.SecuCode = c.IDX_SYMBOL 
            AND d.TradingDay = a.END_DATE 
        WHERE
            1 = 1 
            AND a.END_DATE = "{update_date}" 
            AND c.BEGIN_DATE <= "{update_date}" AND ifnull( c.END_DATE, '2099-12-31' ) > "{update_date}" 
            AND b.REPORT_DATE = (
            SELECT
                max( REPORT_DATE ) 
            FROM
                fund_type_own 
            WHERE
            PUBLISH_DATE <= "{update_date}")
        )SELECT
            a.*,
            a.LOG_ALPHA + ifnull( c.CUM_LOG_ALPHA, 0 ) AS CUM_LOG_ALPHA,
            exp(( a.LOG_ALPHA + ifnull( c.CUM_LOG_ALPHA, 0 ))/ 100 ) AS CUM_ALPHA_NAV 
        FROM
            a
            LEFT JOIN md_tradingdaynew b ON a.END_DATE = b.TRADE_DT 
            AND b.SECU_MARKET = 83
            LEFT JOIN fund_derivatives_enhanced_index_alpha c ON c.END_DATE = b.PREV_TRADE_DATE 
            AND c.TICKER_SYMBOL = a.TICKER_SYMBOL
        """
    query_from_remote_upsert_into_local(
        query_sql,
        table="fund_derivatives_enhanced_index_alpha",
        query_db_conn=DB_CONN_JJTG_DATA,
    )


def update_fund_derivatives_enhanced_index_performance(
    update_date: str = None, ticker_symbol_list: list = None
):
    """
    更新指数增强基金表现

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    result_df = cal_enhanced_index_performance(
        end_date=update_date, ticker_symbol_list=ticker_symbol_list
    )
    DB_CONN_JJTG_DATA.upsert(
        result_df, table="fund_derivatives_enhanced_index_performance"
    )


def update_fund_derivatives_enhanced_index_performance_rank(update_date: str = None):
    """
    更新指数增强基金表现排名

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    period_list = ["1M", "2M", "3M", "6M", "9M", "1Y", "2Y", "3Y", "YTD"]
    result_list = []
    for period in period_list:
        query_sql = f"""
        WITH c as (
        select TICKER_SYMBOL, IDX_SHORT_NAME, IS_EXE
            from fund_tracking_idx
        union 
                select TICKER_SYMBOL, IDX_SHORT_NAME, IS_EXE
        from fund_tracking_idx_own 
        ), a AS (
            SELECT
                a.END_DATE,
                a.TICKER_SYMBOL,
                c.IDX_SHORT_NAME,
                a.INDICATOR,
                a.{period} 
            FROM
                fund_derivatives_enhanced_index_performance a
                JOIN c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
            WHERE
                1 = 1 
                AND c.IS_EXE = 1 
                AND a.{period} IS NOT NULL 
                and a.END_DATE = '{update_date}'
            ) SELECT
            END_DATE,
            TICKER_SYMBOL,
            IDX_SHORT_NAME,
            INDICATOR,
            percent_rank() OVER (PARTITION BY END_DATE, IDX_SHORT_NAME, INDICATOR ORDER BY {period} DESC ) * 100 AS {period}_RANK
        FROM
            a
        ORDER BY
            END_DATE,
            IDX_SHORT_NAME
        """
        df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        df.loc[df["INDICATOR"].isin(["ANNUAL_VOL", "MAXDD"]), f"{period}_RANK"] = (
            100
            - df.loc[df["INDICATOR"].isin(["ANNUAL_VOL", "MAXDD"]), f"{period}_RANK"]
        )
        result_list.append(df)

    result_df = reduce(lambda x, y: pd.merge(x, y, how="outer"), result_list)
    DB_CONN_JJTG_DATA.upsert(
        result_df, "fund_derivatives_enhanced_index_performance_rank"
    )


def update_bond_derivatives_temperature(update_date: str = None):
    """
    更新债券温度衍生表

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT

    sql_query = """
        SELECT
            date_format(EndDate, "%Y%m%d") AS TRADE_DT,
            CurveCode AS B_ANAL_CURVENUMBER,
            YearsToMaturity AS B_ANAL_CURVETERM,
            Yield * 100 AS B_ANAL_YIELD 
        FROM
            `bond_cbyieldcurve` 
        WHERE
            1 = 1 
            AND CurveCode IN ( 69, 44, 281, 280, 290, 10, 195, 74, 219 ) 
            AND `YieldTypeCode` = 1 
            AND `YearsToMaturity` IN ( 1, 3, 5, 10 ) 
        ORDER BY
            B_ANAL_CURVENUMBER,
            B_ANAL_CURVETERM,
            TRADE_DT
    """
    bond_ytm = DB_CONN_JY.exec_query(sql_query)
    code_dict = {
        69: 1102,
        44: 1262,
        281: 2402,
        280: 2392,
        290: 2452,
        10: 1232,
        195: 2142,
        74: 1872,
        219: 1842,
    }
    code_name = {
        69: "中债中短期票据收益率曲线(AAA)",
        44: "中债企业债收益率曲线(AAA)",
        281: "中债商业银行二级资本债收益率曲线(AA+)",
        280: "中债商业银行二级资本债收益率曲线(AAA-)",
        290: "中债商业银行同业存单收益率曲线(AAA)",
        10: "中债国债收益率曲线",
        195: "中债国开债收益率曲线",
        74: "中债城投债收益率曲线(AA)",
        219: "中债城投债收益率曲线(AAA)",
    }
    bond_ytm["B_ANAL_CURVENAME"] = bond_ytm["B_ANAL_CURVENUMBER"].map(code_name)
    bond_ytm["B_ANAL_CURVENUMBER"] = bond_ytm["B_ANAL_CURVENUMBER"].map(code_dict)

    # 获取国债收益率
    government_bond_ytm = bond_ytm.query("B_ANAL_CURVENUMBER==1232")[
        ["TRADE_DT", "B_ANAL_CURVETERM", "B_ANAL_YIELD"]
    ]
    government_bond_ytm.rename(
        columns={"B_ANAL_YIELD": "GOVERNMENT_BOND_YIELD"}, inplace=True
    )

    # 计算信用利差
    bond_ytm = bond_ytm.merge(government_bond_ytm)
    bond_ytm["CREDIT_SPREAD"] = (
        bond_ytm["B_ANAL_YIELD"] - bond_ytm["GOVERNMENT_BOND_YIELD"]
    )
    # 计算到期收益与信用利差分位数
    bond_ytm[["YIELD_RANK_PCT", "CREDIT_SPREAD_RANK_PCT"]] = (
        bond_ytm.groupby(by=["B_ANAL_CURVENUMBER", "B_ANAL_CURVETERM"])[
            ["B_ANAL_YIELD", "CREDIT_SPREAD"]
        ]
        .rolling(850)
        .rank(pct=True, ascending=False)
        .reset_index()
        .set_index("level_2")
    )[["B_ANAL_YIELD", "CREDIT_SPREAD"]] * 100
    # 温度均值
    bond_ytm["TEMPERATURE"] = (
        bond_ytm["YIELD_RANK_PCT"] + bond_ytm["CREDIT_SPREAD_RANK_PCT"]
    ) / 2

    df_upsert = bond_ytm.query(f"TRADE_DT >= '{update_date}'")

    DB_CONN_JJTG_DATA.upsert(
        df_to_upsert=df_upsert, table="bond_derivatives_temperature"
    )


def __update_fund_holding_sector(update_date: str = None, if_q=0):
    """
    更新基金持仓行业and板块

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    report_date = dm.get_report_date(update_date, 1)[0]
    q_dict = {
        0: "",
        1: "_q",
    }

    q_str_dict = {
        0: "",
        1: "INDUSTRY_RATIO_IN_TOP10,",
    }

    query_sql = f"""
    SELECT
        REPORT_DATE,
        TICKER_SYMBOL,
        INDUSTRY_NAME,
        INDUSTRY_RATIO_IN_NA,
        INDUSTRY_RATIO_IN_EQUITY,
        {q_str_dict[if_q]}
	    MARKET_VALUE 
    FROM
        fund_holding_industry{q_dict[if_q]}
    WHERE
        1 = 1 
        AND level_num = 1 
        and report_date = '{report_date}'
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    if df.empty:
        return None

    sector = dm.get_sector_industry_map()

    sector_df = df.merge(sector)
    col = ["INDUSTRY_RATIO_IN_NA", "INDUSTRY_RATIO_IN_EQUITY", "MARKET_VALUE"]
    rename_dict = {
        "INDUSTRY_RATIO_IN_NA": "SECTOR_RATIO_IN_NA",
        "INDUSTRY_RATIO_IN_EQUITY": "SECTOR_RATIO_IN_EQUITY",
    }
    if if_q == 1:
        col.append("INDUSTRY_RATIO_IN_TOP10")
        rename_dict["INDUSTRY_RATIO_IN_TOP10"] = "SECTOR_RATIO_IN_TOP10"

    sector_df = (
        sector_df.groupby(by=["REPORT_DATE", "TICKER_SYMBOL", "SECTOR"])[col]
        .sum()
        .reset_index()
    )
    sector_df = sector_df.rename(columns=rename_dict)
    DB_CONN_JJTG_DATA.upsert(sector_df, table=f"fund_holding_sector{q_dict[if_q]}")


def update_fund_holding_sector(update_date: str = None):
    """
    更新基金持仓行业and板块

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT

    for if_q in [0, 1]:
        __update_fund_holding_sector(update_date, if_q)


def __update_fund_holding_industry(update_date: str = None, if_q=0):
    """
    更新基金持仓行业

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    q_dict = {
        0: "",
        1: "_q",
    }
    report_date = report_date = dm.get_report_date(update_date, 1)[0]
    query_sql = f"""
    SELECT
        a.REPORT_DATE,
        c.TICKER_SYMBOL,
        a.RATIO_IN_NA * 100 as RATIO_IN_NA,
        a.HOLDING_TICKER_SYMBOL,
        a.EXCHANGE_CD,
        a.MARKET_VALUE 
    FROM
        fund_holdings{q_dict[if_q]} a 
        JOIN fund_info c on c.fund_id = a.fund_id
    WHERE
        1 = 1 
        AND a.security_type = 'E'
        and a.report_date = '{report_date}'
    """

    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    if df.empty:
        return None
    fund_asset = dm.get_fund_asset_own(report_date=report_date)[
        ["REPORT_DATE", "TICKER_SYMBOL", "EQUITY_MARKET_VALUE"]
    ]

    for level_num in range(1, 4):
        industry = dm.get_stock_sw_industry_21(report_date, level_num=level_num)
        industry.rename(
            columns={"TICKER_SYMBOL": "HOLDING_TICKER_SYMBOL"}, inplace=True
        )
        industry_df = df.merge(industry)
        industry_df = (
            industry_df.groupby(by=["REPORT_DATE", "TICKER_SYMBOL", "INDUSTRY_NAME"])[
                ["RATIO_IN_NA", "MARKET_VALUE"]
            ]
            .sum()
            .reset_index()
            .merge(fund_asset)
        )
        industry_df["INDUSTRY_RATIO_IN_EQUITY"] = (
            industry_df["MARKET_VALUE"] / industry_df["EQUITY_MARKET_VALUE"] * 100
        )
        industry_df = industry_df.rename(
            columns={"RATIO_IN_NA": "INDUSTRY_RATIO_IN_NA"}
        ).drop(columns=["EQUITY_MARKET_VALUE"])
        industry_df["LEVEL_NUM"] = level_num
        if if_q:
            industry_df_q = (
                industry_df.groupby(by=["REPORT_DATE", "TICKER_SYMBOL"])[
                    "INDUSTRY_RATIO_IN_NA"
                ]
                .sum()
                .reset_index()
            )
            industry_df_q = industry_df_q.rename(
                columns={"INDUSTRY_RATIO_IN_NA": "TOP_10_IN_NA"}
            )
            industry_df = industry_df.merge(
                industry_df_q, on=["REPORT_DATE", "TICKER_SYMBOL"]
            )
            industry_df["INDUSTRY_RATIO_IN_TOP10"] = (
                industry_df["INDUSTRY_RATIO_IN_NA"] / industry_df["TOP_10_IN_NA"] * 100
            )
            industry_df.drop(columns=["TOP_10_IN_NA"], inplace=True)

        DB_CONN_JJTG_DATA.upsert(industry_df, f"fund_holding_industry{q_dict[if_q]}")


def update_fund_holding_industry(update_date: str = None):
    """
    更新基金持仓行业

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT

    for if_q in [0, 1]:
        __update_fund_holding_industry(update_date, if_q)


def update_fund_holding_industry_sector_q(update_date: str = None):
    """
    更新基金持仓行业and板块

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT

    report_date = dm.get_report_date(update_date, 1)[0]
    print(report_date)
    query_sql = f"""
        WITH t1 AS ( 
        SELECT 
            DISTINCT REPORT_DATE, FUND_ID 
        FROM fund_holdings_q 
        WHERE 
            1 = 1 
            AND REPORT_DATE = '{report_date}' 
            AND update_TIME >= '{report_date}' 
            AND security_type = 'E' 
        ) SELECT
        a.REPORT_DATE,
        c.TICKER_SYMBOL,
        a.RATIO_IN_NA,
        a.HOLDING_TICKER_SYMBOL,
        a.EXCHANGE_CD,
        a.MARKET_VALUE 
        FROM
            t1
            JOIN fund_holdings_q a ON t1.REPORT_DATE = a.REPORT_DATE 
            AND t1.FUND_ID = a.FUND_ID
            JOIN fund_info c ON c.fund_id = a.fund_id 
        WHERE
            1 = 1 
            AND a.security_type = 'E'
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    if df.empty:
        return None

    industry = dm.get_stock_sw_industry_21(report_date)
    sector = dm.get_sector_industry_map()
    fund_asset = dm.get_fund_asset_own(report_date=report_date)[
        ["REPORT_DATE", "TICKER_SYMBOL", "EQUITY_MARKET_VALUE"]
    ]
    industry.rename(columns={"TICKER_SYMBOL": "HOLDING_TICKER_SYMBOL"}, inplace=True)
    industry_df = df.merge(industry)
    sector_df = industry_df.merge(sector)

    industry_df = (
        industry_df.groupby(by=["REPORT_DATE", "TICKER_SYMBOL", "INDUSTRY_NAME"])[
            ["RATIO_IN_NA", "MARKET_VALUE"]
        ]
        .sum()
        .reset_index()
        .merge(fund_asset)
    )
    industry_df["INDUSTRY_RATIO_IN_EQUITY"] = (
        industry_df["MARKET_VALUE"] / industry_df["EQUITY_MARKET_VALUE"] * 100
    )
    industry_df_top_10_mv = (
        industry_df.groupby(by=["REPORT_DATE", "TICKER_SYMBOL"])[["MARKET_VALUE"]]
        .sum()
        .reset_index()
        .rename(columns={"MARKET_VALUE": "MARKET_VALUE_TOP10"})
    )
    industry_df = industry_df.merge(industry_df_top_10_mv)
    industry_df["INDUSTRY_RATIO_IN_TOP10"] = (
        industry_df["MARKET_VALUE"] / industry_df["MARKET_VALUE_TOP10"] * 100
    )
    sector_df = (
        sector_df.groupby(by=["REPORT_DATE", "TICKER_SYMBOL", "SECTOR"])[
            ["RATIO_IN_NA", "MARKET_VALUE"]
        ]
        .sum()
        .reset_index()
        .merge(fund_asset)
    )
    sector_df["SECTOR_RATIO_IN_EQUITY"] = (
        sector_df["MARKET_VALUE"] / sector_df["EQUITY_MARKET_VALUE"] * 100
    )
    sector_df_top_10_mv = (
        sector_df.groupby(by=["REPORT_DATE", "TICKER_SYMBOL"])[["MARKET_VALUE"]]
        .sum()
        .reset_index()
        .rename(columns={"MARKET_VALUE": "MARKET_VALUE_TOP10"})
    )
    sector_df = sector_df.merge(sector_df_top_10_mv)
    sector_df["SECTOR_RATIO_IN_TOP10"] = (
        sector_df["MARKET_VALUE"] / sector_df["MARKET_VALUE_TOP10"] * 100
    )

    sector_df = sector_df.rename(columns={"RATIO_IN_NA": "SECTOR_RATIO_IN_NA"}).drop(
        columns=["EQUITY_MARKET_VALUE", "MARKET_VALUE_TOP10"]
    )
    industry_df = industry_df.rename(
        columns={"RATIO_IN_NA": "INDUSTRY_RATIO_IN_NA"}
    ).drop(columns=["EQUITY_MARKET_VALUE", "MARKET_VALUE_TOP10"])

    DB_CONN_JJTG_DATA.upsert(sector_df, "fund_holding_sector_q")
    DB_CONN_JJTG_DATA.upsert(industry_df, "fund_holding_industry_q")


def update_qt_tradingdaynew(update_date: str = None):
    """
    更新基金业绩表现

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT

    query_sql = f"""
    SELECT
        TradingDate AS TRADE_DT,
    CASE
            IfTradingDay 
            WHEN 1 THEN
            1 ELSE 0 
        END AS IF_TRADING_DAY,
    SecuMarket AS SECU_MARKET,
    CASE
            IfWeekEnd 
            WHEN 1 THEN
            1 ELSE 0 
        END AS IF_WEEK_END,
    CASE
            IfMonthEnd 
            WHEN 1 THEN
            1 ELSE 0 
        END AS IF_MONTH_END,
    CASE
            IfQuarterEnd 
            WHEN 1 THEN
            1 ELSE 0 
        END AS IF_QUARTER_END,
    CASE
            IfYearEnd 
            WHEN 1 THEN
            1 ELSE 0 
        END AS IF_YEAR_END,
        XGRQ AS UPDATE_TIME 
    FROM
        qt_tradingdaynew 
    WHERE
        1 = 1 
        # AND XGRQ >= '{update_date}'
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table="md_tradingdaynew",
        query_db_conn=DB_CONN_JY,
    )
    dates_dict = {
        "PREV_TRADE": "TRADING_DAY",
        "WEEK_END": "WEEK_END",
        "MONTH_END": "MONTH_END",
        "QUARTER_END": "QUARTER_END",
        "YEAR_END": "YEAR_END",
    }
    result_list = []
    for k, v in dates_dict.items():
        query_sql = f"""
        WITH a AS (
            SELECT
                TRADE_DT,
                SECU_MARKET,
                LAG( TRADE_DT, 1 ) over ( PARTITION BY SECU_MARKET ORDER BY TRADE_DT ) AS {k}_DATE 
            FROM
                `chentiancheng`.`md_tradingdaynew` 
            WHERE
                1 = 1 
                AND IF_{v} = 1
            ) SELECT
            b.TRADE_DT,
            b.SECU_MARKET,
            a.{k}_DATE 
        FROM
            md_tradingdaynew b
            JOIN a ON a.SECU_MARKET = b.SECU_MARKET 
        WHERE
            1 = 1 
            AND b.TRADE_DT > a.{k}_DATE  
            AND b.TRADE_DT <= a.TRADE_DT 
            # and b.update_time >= '{update_date}'
        ORDER BY
            SECU_MARKET,
            TRADE_DT
        """
        df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        result_list.append(df)
    result_df = reduce(lambda x, y: pd.merge(x, y, how="outer"), result_list)

    if not result_df.empty:
        DB_CONN_JJTG_DATA.upsert(result_df, table="md_tradingdaynew")


def update_temperature_stock(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        a.TradingDay AS TRADE_DT,
        b.SecuCode AS TICKER_SYMBOL,
    CASE
            b.SecuMarket 
            WHEN 83 THEN
            CONCAT( SecuCode, ".SH" ) ELSE CONCAT( SecuCode, ".SZ" ) 
        END AS S_INFO_WINDCODE,
        b.ChiNameAbbr AS S_INFO_NAME,
        a.PE_TTM,
        a.DividendRatio AS DIVIDEND_YIELD,
        a.PB_LF,
        ( 1 / a.PE_TTM - c.Yield ) * 100 AS ERP_MINUS,
        ( 1 / a.PE_TTM ) / c.Yield AS ERP_DIV,
        DividendRatio - c.Yield * 100 AS DIVIDEND_SPREAD_MINUS,
        DividendRatio / c.Yield / 100 AS DIVIDEND_SPREAD_DIV 
    FROM
        lc_indexderivative a
        JOIN secumainall b ON a.IndexCode = b.InnerCode
        JOIN bond_cbyieldcurve c ON c.EndDate = a.TradingDay 
        AND c.Curvecode = 10 
        AND c.YieldTypeCode = 1 
        AND c.YearsToMaturity = 10 
    WHERE
        1 = 1 
        AND a.TradingDay = '{update_date}' 
        AND b.DelistingDate IS NULL 
        AND b.SecuCode IN (
            '399372',
            '399373',
            '399374',
            '399375',
            '399376',
            '399377',
            '881001',
            '000985',
            '000300',
            '000905',
            '000906',
            '000852',
            '399006',
            '000016',
            '399303',
            '399673',
            '399295',
            '399296',
            '000922',
            '931355',
            '000934',
            '000998',
            '399808',
            '000933',
            '399932' 
        )
    """

    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table="temperature_stock_index",
        query_db_conn=DB_CONN_JY,
    )
    start_date_list = ["20100101", "20150101"]
    for start_date in start_date_list:
        query_sql = """
        INSERT INTO temperature_pct_rank(
            END_DATE, START_DATE, TICKER_SYMBOL, 
            S_INFO_NAME, PE_PCT_RANK, ERP_PCT_RANK,
            DIVIDEND_YIELD_RANK, PB_RANK, DIVIDEND_SPREAD_RANK,
            TEMPERATURE
        )
        """
        query_sql += f"""
        WITH a AS (
            SELECT
                TRADE_DT AS TRADE_DT,
                TICKER_SYMBOL AS TICKER_SYMBOL,
                S_INFO_NAME AS S_INFO_NAME,
                round(( percent_rank() OVER ( PARTITION BY TICKER_SYMBOL ORDER BY PE_TTM ) * 100 ), 6 ) AS PE_PCT_RANK,(
                    round( percent_rank() OVER ( PARTITION BY TICKER_SYMBOL ORDER BY ERP_MINUS ), 6 ) * 100 
                    ) AS ERP_PCT_RANK,(
                    round( percent_rank() OVER ( PARTITION BY TICKER_SYMBOL ORDER BY DIVIDEND_YIELD ), 6 ) * 100 
                    ) AS DIVIDEND_YIELD_RANK,(
                    round( percent_rank() OVER ( PARTITION BY TICKER_SYMBOL ORDER BY PB_LF ), 6 ) * 100 
                    ) AS PB_RANK,(
                    round( percent_rank() OVER ( PARTITION BY TICKER_SYMBOL ORDER BY DIVIDEND_SPREAD_MINUS ), 6 ) * 100 
                ) AS DIVIDEND_SPREAD_RANK 
            FROM
                temperature_stock_index 
            WHERE
                1 = 1 
                AND ( TRADE_DT BETWEEN '{start_date}' AND '{update_date}' ) 
            ),
            b AS ( SELECT TICKER_SYMBOL, min( TRADE_DT ) AS START_DATE FROM a GROUP BY TICKER_SYMBOL ) SELECT
            a.TRADE_DT AS END_DATE,
            b.START_DATE,
            a.TICKER_SYMBOL AS TICKER_SYMBOL,
            a.S_INFO_NAME AS S_INFO_NAME,
            a.PE_PCT_RANK AS PE_PCT_RANK,
            a.ERP_PCT_RANK AS ERP_PCT_RANK,
            a.DIVIDEND_YIELD_RANK AS DIVIDEND_YIELD_RANK,
            a.PB_RANK AS PB_RANK,
            a.DIVIDEND_SPREAD_RANK AS DIVIDEND_SPREAD_RANK,
            round(( a.PE_PCT_RANK + 100 - a.ERP_PCT_RANK ) / 2, 6 ) AS TEMPERATURE
        FROM
            a
            JOIN b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND a.TRADE_DT = '{update_date}'
        """
        query_sql += """
        ON DUPLICATE KEY UPDATE
            END_DATE = values(END_DATE), 
            START_DATE = values(START_DATE), 
            TICKER_SYMBOL = values(TICKER_SYMBOL), 
            S_INFO_NAME = values(S_INFO_NAME),
            PE_PCT_RANK = values(PE_PCT_RANK), 
            ERP_PCT_RANK = values(ERP_PCT_RANK),
            DIVIDEND_YIELD_RANK = values(DIVIDEND_YIELD_RANK), 
            PB_RANK = values(PB_RANK), 
            DIVIDEND_SPREAD_RANK = values(DIVIDEND_SPREAD_RANK),
            TEMPERATURE = values(TEMPERATURE)
        """
        DB_CONN_JJTG_DATA.exec_non_query(query_sql)


def update_portfolio_benchmark_ret(update_date: str = None):
    if update_date is None:
        update_date = dm.offset_trade_dt(LAST_TRADE_DT, 2)

    query_sql = f"""
    SELECT
        TRADE_DT,
        TICKER_SYMBOL,
        (S_DQ_CLOSE/S_DQ_PRECLOSE - 1)*100 AS RETURN_RATE,
        log( S_DQ_CLOSE/S_DQ_PRECLOSE) * 100 AS LOG_RETURN_RATE 
    FROM
        fund_index_eod 
    WHERE
        1 = 1 
        AND TICKER_SYMBOL IN ( '930950', '930609', 'H11025', '885001', '885000', '885008', '885062', '885007', '885006', '885070' ) 
        AND trade_dt >= "{update_date}" UNION
    SELECT
        TRADE_DT,
        TICKER_SYMBOL,
        (S_DQ_CLOSE/S_DQ_PRECLOSE - 1)*100 AS RETURN_RATE,
        log( S_DQ_CLOSE/S_DQ_PRECLOSE) * 100 AS LOG_RETURN_RATE 
    FROM
        aindex_eod_prices 
    WHERE
        1 = 1 
        AND TRADE_DT >= "{update_date}" UNION
    SELECT
        TRADE_DT,
        TICKER_SYMBOL,
        (S_DQ_CLOSE/S_DQ_PRECLOSE - 1)*100 AS RETURN_RATE,
        log( S_DQ_CLOSE/S_DQ_PRECLOSE) * 100 AS LOG_RETURN_RATE 
    FROM
        bond_chinabondindexquote 
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = 'CBA00211' 
        AND TRADE_DT >= "{update_date}" UNION
    SELECT
        a.TRADE_DT,
        'B00009' AS TICKER_SYMBOL,
        ( a.IndexDD / c.IndexDD - 1 )* 100 AS RETURN_RATE,
        log( a.IndexDD / c.IndexDD ) * 100 AS LOG_RETURN_RATE 
    FROM
        qt_interestrateindex a
        JOIN md_tradingdaynew b ON b.TRADE_DT = a.TRADE_DT 
        AND b.SECU_MARKET = 83
        JOIN qt_interestrateindex c ON c.TRADE_DT = b.PREV_TRADE_DATE 
    WHERE
        1 = 1 
        AND a.TRADE_DT >= "{update_date}" UNION
    SELECT
        a.TRADE_DT,
        'B00003' AS TICKER_SYMBOL,
        ( a.IndexTD1Y / c.IndexTD1Y - 1 )* 100 AS RETURN_RATE,
        log( a.IndexTD1Y / c.IndexTD1Y ) * 100 AS LOG_RETURN_RATE 
    FROM
        qt_interestrateindex a
        JOIN md_tradingdaynew b ON b.TRADE_DT = a.TRADE_DT 
        AND b.SECU_MARKET = 83
        JOIN qt_interestrateindex c ON c.TRADE_DT = b.PREV_TRADE_DATE 
    WHERE
        1 = 1 
        AND a.TRADE_DT >= "{update_date}" UNION
    SELECT
        END_DATE AS TRADE_DT,
        TYPE_NAME AS TICKER_SYMBOL,
        RETURN_RATE,
        ln( 1+RETURN_RATE / 100 ) * 100 AS LOG_RETURN_RATE 
    FROM
        fund_derivatives_inner_fund_ret 
    WHERE
        1 = 1 
        AND END_DATE >= "{update_date}" UNION
    SELECT
        TradingDay AS TRADE_DT,
        SecuCode AS TICKER_SYMBOL,
        ( closeprice / Prevcloseprice - 1 )* 100 AS RETURN_RATE,
        log( closeprice / Prevcloseprice ) * 100 AS LOG_RETURN_RATE 
    FROM
        jy_indexquote 
    WHERE
        1 = 1 
        AND TradingDay >= "{update_date}" union
    SELECT
        TradeDate as TRADE_DT,
        'AU9999' AS TICKER_SYMBOL,
        ChangePCT AS RETURN_RATE,
        log( 1+ChangePCT / 100 )* 100 AS LOG_RETURN_RATE 
    FROM
        `qt_goldtrademarket` 
    WHERE
        1 = 1 
        AND TradeVariety = 3 
        AND DateType = 3
        and TradeDate >= "{update_date}"
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql).dropna()
    DB_CONN_JJTG_DATA.upsert(df, table="portfolio_benchmark_ret")


def update_fund_redeem_fee(update_date: str = None):
    redeem_fee = dm.get_fund_redeem_fee()
    DB_CONN_JJTG_DATA.upsert(redeem_fee, table="fund_redeem_fee")


def update_fund_performance_rank(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    fund_performance_rank_pct = fd.cal_fund_performance_rank(update_date, if_pct=1)
    DB_CONN_JJTG_DATA.upsert(
        fund_performance_rank_pct, table="fund_performance_rank_pct"
    )
    fund_performance_rank = fd.cal_fund_performance_rank(update_date, if_pct=0)
    DB_CONN_JJTG_DATA.upsert(fund_performance_rank, table="fund_performance_rank")


def update_derivatives_db(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    func_list = [
        update_barra_data,
        update_fund_redeem_fee,
        update_fund_adj_nav,
        update_fund_index_description,
        update_fund_deriavetes_inner_fund_ret,
        update_fund_derivatives_fund_log_alpha,
        update_fund_derivatives_enhanced_index_alpha,
        update_fund_derivatives_enhanced_index_performance,
        update_fund_derivatives_enhanced_index_performance_rank,
        update_bond_derivatives_temperature,
        update_fund_holding_industry,
        update_fund_holding_sector,
        update_qt_tradingdaynew,
        update_temperature_stock,
        update_portfolio_benchmark_ret,
        # update_fund_performance_rank,
    ]

    for i, func in enumerate(func_list, start=1):
        try:
            for _ in range(5):
                func(update_date=update_date)
                print(f"{func.__name__}完成写入")
                print("==" * 35)
                # logging.info(f"成功{i}:{func.__name__}")
                # logging.info("==" * 35)
                break
        except Exception as e:
            logging.warning(f"失败{i}:{func.__name__},原因是:{e}")
            print(f"失败{i}:{func.__name__},原因是:{e}")
            print("!!" * 35)
            logging.info("!!" * 35)

    date = dm.get_now()
    mail_sender = MailSender()
    mail_sender.message_config_content(
        from_name="陈天成", subject=f"【数据库更新】更新完成{date}"
    )
    mail_sender.send_mail(receivers=["569253615@qq.com"])


if __name__ == "__main__":
    update_fund_adj_nav()
    # update_temperature_stock("20240417")
    # update_fund_derivatives_enhanced_index_performance()
    # dts = dm.get_period_end_date(start_date="20240501", end_date="20240711", period="d")
    # for dt in dts:
    #     print(dt)
    #     update_portfolio_benchmark_ret(dt)
