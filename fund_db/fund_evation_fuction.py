import datetime
import math
import os
import sys
import warnings

import numpy as np
import pandas as pd
from docx import Document, oxml
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING, WD_PARAGRAPH_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Inches, Pt, RGBColor

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_JY, DB_CONN_JY_LOCAL
from quant_utils.performance import Performance
from quant_utils.utils import make_dirs

warnings.simplefilter(action="ignore", category=FutureWarning)


def get_related_code(ticker_symbol):
    query_sql = f"""
    SELECT
        f.SecuCode AS init_fund_code,
        g.SecuAbbr AS init_fund_name,
        d.MS AS first_link_type,
        c.SecuCode AS linked_code,
        c.SecuAbbr AS linked_name 
    FROM
        mf_coderelationshipnew a
        JOIN secumain b ON a.InnerCode = b.InnerCode
        JOIN secumain c ON c.InnerCode = a.RelatedInnerCode
        JOIN CT_SystemConst d ON a.CodeDefine = d.DM 
        AND d.LB = 1350
        JOIN mf_fundarchives f ON f.MainCode = b.SecuCode
        JOIN secumain g ON g.InnerCode = f.InnerCode 
    WHERE
        1 = 1 
        AND a.IfEffected = 1 
        AND a.CodeDefine = 21 
        AND f.SecuCode != c.SecuCode 
        AND f.ExpireDate IS NULL 
        AND f.StartDate IS NOT NULL 
        AND f.Secucode = '{ticker_symbol}' UNION
    SELECT
        f.SecuCode AS init_fund_code,
        g.SecuAbbr AS init_fund_name,
        '同一基金分级关联' AS first_link_type,
        f.MainCode AS linked_code,
        j.SecuAbbr AS linked_name 
    FROM
        mf_fundarchives f
        JOIN secumain g ON g.InnerCode = f.InnerCode
        JOIN mf_fundarchives h ON f.MainCode = h.SecuCode
        JOIN secumain j ON h.InnerCode = j.InnerCode 
    WHERE
        1 = 1 
        AND f.MainCode != f.SecuCode 
        AND f.ExpireDate IS NULL 
        AND f.StartDate IS NOT NULL 
        AND f.Secucode = '{ticker_symbol}' UNION
    SELECT
        a.SecuCode AS init_fund_code,
        b.SecuAbbr AS init_fund_name,
        '同一基金分级关联' AS first_link_type,
        NULL AS linked_code,
        NULL AS linked_name 
    FROM
        mf_fundarchives a
        JOIN secumain b ON a.InnerCode = b.InnerCode 
    WHERE
        1 = 1 
        AND a.SecuCode = a.MainCode 
        AND a.ExpireDate IS NULL 
        AND a.StartDate IS NOT NULL 
        AND a.Secucode = '{ticker_symbol}' 
        AND a.InnerCode NOT IN (
        SELECT
            a.InnerCode 
        FROM
            mf_coderelationshipnew a
            JOIN mf_fundarchives b ON a.RelatedInnerCode = b.InnerCode 
        WHERE
            1 = 1 
            AND a.IfEffected = 1 
            AND a.CodeDefine = 21 
        AND b.ExpireDate IS NULL 
        )
    """
    return DB_CONN_JY.exec_query(query_sql)


def get_fund_manager(ticker_symbol, end_date):
    query_sql = f"""
    WITH b AS (
        SELECT
            `fund_manager_info`.`PERSON_ID` AS `PERSON_ID`,
            `fund_manager_info`.`NAME` AS `NAME`,
            min( `fund_manager_info`.`ACCESSION_DATE` ) AS `ACCESSION_DATE`,
            max(
            ifnull( `fund_manager_info`.`DIMISSION_DATE`, '{end_date}' )) AS `DIMISSION_DATE`,
            round((( to_days( max( ifnull( `fund_manager_info`.`DIMISSION_DATE`, "{end_date}" ))) - to_days( min( `fund_manager_info`.`ACCESSION_DATE` ))) / 365 ), 2 ) AS `MANAGER_DURATION` 
        FROM
            `fund_manager_info` 
        GROUP BY
            `fund_manager_info`.`PERSON_ID`,
            `fund_manager_info`.`NAME` 
        ) SELECT
        `a`.`TICKER_SYMBOL` AS `TICKER_SYMBOL`,
        group_concat( `a`.`NAME` SEPARATOR ';' ) AS `MANAGER_NAME`,
    group_concat( `b`.`MANAGER_DURATION` SEPARATOR ';' ) AS `MANAGER_DURATION` 
    FROM
        (
            `fund_manager_info` `a`
            JOIN `b` ON ((
                    `b`.`PERSON_ID` = `a`.`PERSON_ID` 
                ))) 
    WHERE
        ((
                '{end_date}' BETWEEN a.ACCESSION_DATE 
                AND IFNULL( a.DIMISSION_DATE, '20991231' ) 
                ) 
        AND ( `a`.`POSITION` = 'FM' )) 
        AND TICKER_SYMBOL = '{ticker_symbol}' 
    GROUP BY
        `a`.`TICKER_SYMBOL`,
        `a`.`SEC_SHORT_NAME`
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_management_company(ticker_symbol):
    query_sql = f"""
    select 
    TICKER_SYMBOL, 
    MANAGEMENT_COMPANY_NAME 
    from 
    fund_info 
    where TICKER_SYMBOL = '{ticker_symbol}'
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    return df["MANAGEMENT_COMPANY_NAME"].values[0]


def get_fund_type_asset_rank(report_date):
    query_sql = f"""
    WITH a AS (
        SELECT
            b.REPORT_DATE,
            a.LEVEL_SUM,
            d.MANAGEMENT_COMPANY_NAME,
            round( sum( c.NET_ASSET )/ 100000000, 4 ) AS NET_ASSET 
        FROM
            fund_type_sum a
            JOIN fund_type_own b ON a.LEVEL_1 = b.LEVEL_1 
            AND a.level_2 = b.level_2
            JOIN fund_asset_own c ON c.TICKER_SYMBOL = b.TICKER_SYMBOL 
            AND b.REPORT_DATE = c.REPORT_DATE
            JOIN fund_info d ON d.TICKER_SYMBOL = c.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND b.report_date = '{report_date}' 
            AND d.is_main = 1 
        GROUP BY
            a.LEVEL_SUM,
            d.MANAGEMENT_COMPANY_NAME UNION
        SELECT
            b.REPORT_DATE,
            "指数增强" AS LEVEL_SUM,
            d.MANAGEMENT_COMPANY_NAME,
            round( sum( c.NET_ASSET )/ 100000000, 4 ) AS NET_ASSET 
        FROM
            fund_type_own b
            JOIN fund_asset_own c ON c.TICKER_SYMBOL = b.TICKER_SYMBOL 
            AND b.REPORT_DATE = c.REPORT_DATE
            JOIN fund_info d ON d.TICKER_SYMBOL = c.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND b.report_date = '{report_date}' 
            AND b.level_2 IN ( '指数增强', '国际(QDII)增强指数型股票基金' ) 
            AND d.is_main = 1 
        GROUP BY
            d.MANAGEMENT_COMPANY_NAME 
        ) SELECT
        a.*,
        ROW_NUMBER() over ( PARTITION BY LEVEL_SUM ORDER BY NET_ASSET DESC ) AS `排名` 
    FROM
        a 
    WHERE
        1 = 1
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_asset(ticker_symbol, trade_dt):
    select_sql = f"""
    SELECT
        REPORT_DATE,
        TICKER_SYMBOL,
        NET_ASSET / 100000000 AS NET_ASSET 
    FROM
        fund_asset_own 
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = '{ticker_symbol}' 
        AND REPORT_DATE = (
        SELECT
            max( REPORT_DATE ) 
        FROM
            fund_asset_own 
        WHERE
            TICKER_SYMBOL = '{ticker_symbol}' 
        AND PUBLISH_DATE <= '{trade_dt}' 
    )
    """
    return DB_CONN_JJTG_DATA.exec_query(select_sql)["NET_ASSET"].values[0]


def get_evaluated_date(ticker_symbol, end_date):
    query_sql = f"""
    WITH a AS (
        SELECT
            min( ACCESSION_DATE ) AS DATE 
        FROM
            fund_manager_info 
        WHERE
            1 = 1 
            AND TICKER_SYMBOL = '{ticker_symbol}' 
            AND  ACCESSION_DATE >= '{end_date}'
            and ifnull(DIMISSION_DATE,'20991231') < '{end_date}' 
            AND POSITION = "FM" UNION
        SELECT
            START_DATE AS DATE 
        FROM
            portfolio_dates 
        WHERE
            1 = 1 
            AND END_DATE = '{end_date}' 
            AND PORTFOLIO_NAME = 'ALL' 
            AND DATE_NAME = '近3年' 
        ) SELECT
        DATE_FORMAT(max(DATE),"%Y%m%d") AS DATE 
    FROM
        a
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["DATE"].values[0]


def get_other_evaluated_date(ticker_symbol, end_date):

    query_sql = f"""
    WITH a AS (
        SELECT
            min( ESTABLISH_DATE ) AS DATE 
        FROM
            fund_info 
        WHERE
            1 = 1 
            AND TICKER_SYMBOL = '{ticker_symbol}' 
            AND  ESTABLISH_DATE >= '{end_date}'
            and ifnull(EXPIRE_DATE,'20991231') < '{end_date}' 
            AND EXPIRE_DATE IS NULL 
            UNION
        SELECT
            START_DATE AS DATE 
        FROM
            portfolio_dates 
        WHERE
            1 = 1 
            AND END_DATE = '{end_date}' 
            AND PORTFOLIO_NAME = 'ALL' 
            AND DATE_NAME = '近3年' 
        ) SELECT
        DATE_FORMAT( max( DATE ), "%Y%m%d" ) AS DATE 
    FROM
        a
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["DATE"].values[0]


def get_fund_annualised_alpha(ticekr_symbol, start_date, end_date):

    fund_type = get_fund_type(ticekr_symbol, end_date)

    if fund_type["LEVEL_2"].values[0] not in (
        "指数增强",
        "国际(QDII)增强指数型股票基金",
    ):
        nav = dm.get_fund_adj_nav(
            ticker_symbol=ticekr_symbol, end_date=end_date, start_date=start_date
        )
        bench_nav = dm.get_index_close(
            ticker_symbol="000300", end_date=end_date, start_date=start_date
        )
        nav = nav.merge(
            bench_nav[["TRADE_DT", "S_DQ_CLOSE"]], on="TRADE_DT", how="left"
        )
    else:
        nav = get_fund_nav_benchmark(
            ticker_symbol=ticekr_symbol, end_date=end_date, start_date=start_date
        )
    nav = nav.set_index("TRADE_DT")
    # print(nav)
    fund_annualised_ret = Performance(nav["ADJUST_NAV"]).annual_return()
    bench_annualised_ret = Performance(nav["S_DQ_CLOSE"]).annual_return()
    ir = Performance(nav["ADJUST_NAV"], nav["S_DQ_CLOSE"]).IR()
    fund_maxdd = Performance(nav["ADJUST_NAV"]).max_drawdown()
    bench_maxdd = Performance(nav["S_DQ_CLOSE"]).max_drawdown()
    return_dict = {}
    return_dict = {
        "基金年化收益": fund_annualised_ret,
        "基准年化收益": bench_annualised_ret,
        "超额": (fund_annualised_ret - bench_annualised_ret),
        "IR": ir,
        "最大回撤": fund_maxdd,
        "基准最大回撤": bench_maxdd,
        "回撤比值": fund_maxdd / bench_maxdd,
    }
    return return_dict


def get_fund_company_stablility(ticker_symbol, end_date):
    query_sql = f"""
    WITH a AS (
        SELECT
            a.EndDate,
            b.ChiName AS '基金公司',
            b.AbbrChiName '基金公司简称',
            b.CompanyCode,
            a.DimissionInSingleYear AS '离职人数',
            c.NumOfManager '基金经理人数',
            a.DimissionInSingleYear / c.NumOfManager AS '近一年离职率' 
        FROM
            `mf_fcexpanalysis` a
            JOIN LC_InstiArchive b ON a.InvestAdvisorCode = b.CompanyCode
            JOIN MF_FCNumOfManager c ON c.InvestAdvisorCode = a.InvestAdvisorCode 
            AND c.EndDate = a.EndDate 
        WHERE
            a.EndDate = '{end_date}' 
            AND c.TypeName = '全部基金' 
        ORDER BY
            a.DimissionInSingleYear DESC 
        ) SELECT
        `近一年离职率`,
    CASE
            WHEN `近一年离职率` <= 0.1 THEN 5 WHEN ( `近一年离职率` > 0.1 
                AND `近一年离职率` <= 0.2 ) THEN 4 WHEN ( `近一年离职率` > 0.2 
                    AND `近一年离职率` <= 0.3 ) THEN 3 WHEN ( `近一年离职率` > 0.3 
                        AND `近一年离职率` <= 0.4 ) THEN 2 WHEN ( `近一年离职率` > 0.4 
                            AND `近一年离职率` <= 0.5 
                            ) THEN
                            1 ELSE 0 
                        END AS '得分' 
                    FROM
                        a
                        JOIN mf_fundarchives b ON a.CompanyCode = b.InvestAdvisorCode 
                    WHERE
                    1 = 1 
        AND b.SecuCode = '{ticker_symbol}'
    """
    df = DB_CONN_JY_LOCAL.exec_query(query_sql)
    return {
        "近一年离职率": float(df["近一年离职率"].values[0]),
        "得分": df["得分"].values[0],
    }


def get_fund_type(ticker_symbol, end_date):
    query_sql = f"""
    select
	    a.REPORT_DATE,
        a.TICKER_SYMBOL,
        a.LEVEL_1,
        a.LEVEL_2,
        a.LEVEL_3,
        b.LEVEL_SUM 
    FROM
        fund_type_own a
	JOIN fund_type_sum b ON a.LEVEL_1 = b.LEVEL_1 and a.LEVEL_2 = b.LEVEL_2
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = '{ticker_symbol}' 
        AND REPORT_DATE = (
        SELECT
            max( REPORT_DATE ) 
        FROM
            fund_type_own 
        WHERE
            1 = 1 
            AND TICKER_SYMBOL = '{ticker_symbol}' 
        AND PUBLISH_DATE <= '{end_date}' 
        )
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_perf_rank(ticker_symbol, end_date):
    query_sql = f"""
        SELECT
        TRADE_DT,
        TICKER_SYMBOL,
        `LEVEL`,
        F_AVGRETURN_YEAR,
        F_AVGRETURN_TWOYEAR,
        F_AVGRETURN_THREEYEAR,
        F_MAXDOWNSIDE_YEAR,
        F_MAXDOWNSIDE_TWOYEAR,
        F_MAXDOWNSIDE_THREEYEAR,
        F_SHARPRATIO_YEAR,
        F_SHARPRATIO_TWOYEAR,
        F_SHARPRATIO_THREEYEAR 
    FROM
        fund_performance_rank_pct
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = '{ticker_symbol}' 
        AND TRADE_DT = '{end_date}' 
        AND `LEVEL` = 'LEVEL_2'
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    if df.empty:
        raise Exception("No data found")
    if df["F_AVGRETURN_THREEYEAR"].values[0]:
        result = {
            "绝对业绩": float(df["F_AVGRETURN_THREEYEAR"].values[0]),
            "最大回撤": float(df["F_MAXDOWNSIDE_THREEYEAR"].values[0]),
            "夏普比率": float(df["F_SHARPRATIO_THREEYEAR"].values[0]),
        }
    elif df["F_AVGRETURN_TWOYEAR"].values[0]:
        result = {
            "绝对业绩": float(df["F_AVGRETURN_TWOYEAR"].values[0]),
            "最大回撤": float(df["F_MAXDOWNSIDE_TWOYEAR"].values[0]),
            "夏普比率": float(df["F_SHARPRATIO_TWOYEAR"].values[0]),
        }
    else:
        result = {
            "绝对业绩": float(df["F_AVGRETURN_YEAR"].values[0]),
            "最大回撤": float(df["F_MAXDOWNSIDE_YEAR"].values[0]),
            "夏普比率": float(df["F_SHARPRATIO_YEAR"].values[0]),
        }

    return result


def get_fund_type_comapany_rank(tickcer_symbol, end_date):
    fund_type = get_fund_type(tickcer_symbol, end_date)
    fund_type_sum = fund_type["LEVEL_SUM"].values[0]
    fund_company = get_fund_management_company(ticker_symbol=tickcer_symbol)
    fund_type_asset = get_fund_type_asset_rank(fund_type["REPORT_DATE"].values[0])
    if fund_type["LEVEL_2"].values[0] in ("指数增强", "国际(QDII)增强指数型股票基金"):
        return fund_type_asset.query(
            "MANAGEMENT_COMPANY_NAME == @fund_company and LEVEL_SUM =='指数增强'"
        )
    else:
        return fund_type_asset.query(
            "MANAGEMENT_COMPANY_NAME == @fund_company and LEVEL_SUM ==@fund_type_sum"
        )


def get_fund_in_tranch_dates(ticker_symbol):
    query_sql = f"""
    SELECT
        TICKER_SYMBOL,
        DATEDIFF( CURRENT_DATE (), END_DATE ) / 365 AS '入池时间' 
    FROM
        portfolio_basic_products_in_charge 
    WHERE
        1=1
        and TICKER_SYMBOL = '{ticker_symbol}'
        and out_TIME is NULL
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_company_desc(fund_company_name: str) -> pd.DataFrame:
    query_sql = f"""
    SELECT
        MANAGEMENT_COMPANY_NAME,
        TEAM_BUILD,
        ENGAGEMENT,
        COMPLIANCE 
    FROM
        evaluation_fund_company
    WHERE
        1=1
        AND MANAGEMENT_COMPANY_NAME = '{fund_company_name}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_establish_duration(ticker_symbol: str, end_date: str) -> float:
    quert_sql = f"""
    SELECT
        TICKER_SYMBOL,
        DATEDIFF( '{end_date}', ESTABLISH_DATE ) / 365 AS ESTABLISH_DURATION 
    FROM
        fund_info 
    WHERE
        1 = 1 
        AND EXPIRE_DATE IS NULL 
        AND TICKER_SYMBOL = '{ticker_symbol}'
    """
    return DB_CONN_JJTG_DATA.exec_query(quert_sql)["ESTABLISH_DURATION"].values[0]


def get_fund_manager_fund_asset(ticker_symbol: str, end_date: str) -> pd.DataFrame:
    fund_type = get_fund_type(ticker_symbol, end_date=end_date)
    fund_company = get_fund_management_company(ticker_symbol=ticker_symbol)
    fund_manager = (
        get_fund_manager(ticker_symbol=ticker_symbol, end_date=end_date)["MANAGER_NAME"]
        .values[0]
        .split(";")
    )
    asset_list = []
    for manager in fund_manager:
        query_sql = f"""
        SELECT
            round( sum( a.NET_ASSET ) / 100000000, 2 ) AS '同策略规模' 
        FROM
            view_fund_manager_map a
            JOIN fund_type_sum b ON a.LEVEL_1 = b.LEVEL_1 
            AND a.LEVEL_2 = b.LEVEL_2
        WHERE
            1 = 1 
            AND a.IS_MAIN = 1 
            AND a.MANAGER_NAME LIKE '%{manager}%' 
            AND a.MANAGEMENT_COMPANY_NAME = '{fund_company}' 
            AND b.LEVEL_SUM = '{fund_type["LEVEL_SUM"].values[0]}'
        """
        result = DB_CONN_JJTG_DATA.exec_query(query_sql)["同策略规模"].values[0]
        if result:
            asset_list.append(result)
    return max(asset_list)


def check_in_tranche(ticker_symbol: str) -> bool:
    query_sql = f"""
    SELECT
        IF_IN_TRANCHE 
    FROM
        portfolio_basic_products 
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = '{ticker_symbol}'
        and IF_IN_TRANCHE in (0, 1, 2)
    """
    result = DB_CONN_JJTG_DATA.exec_query(query_sql)
    result = 3 if result.empty else result["IF_IN_TRANCHE"].values[0]
    return result


def get_related_code_str(ticker_symbol: str, include_out_tranche: int = 1) -> str:
    related_df = get_related_code(ticker_symbol)
    realted_result = []
    temp_dict = {
        0: "(未入池)",
        1: "",
        2: "(未上架)",
        3: "(未代销)",
    }
    for _, val in related_df.iterrows():
        flag = check_in_tranche(val["init_fund_code"])
        if flag not in (0, 1):
            pass
        else:
            if flag != 1 and include_out_tranche != 1:
                pass
            else:
                realted_result.append(
                    f'{val["init_fund_name"]}:{val["init_fund_code"]}{temp_dict[flag]}'
                )
        if val["linked_code"] is not None:
            flag2 = check_in_tranche(val["linked_code"])
            if flag2 not in (0, 1):
                pass
            else:
                if flag2 != 1 and include_out_tranche != 1:
                    pass
                else:
                    realted_result.append(
                        f'{val["linked_name"]}:{val["linked_code"]}{temp_dict[flag2]}'
                    )
    realted_result = list(set(realted_result))
    realted_result.sort()
    result = ""
    for i in realted_result:
        result += f"{i};\n"
    return result


def get_related_code_list(ticker_symbol: str, include_out_tranche: int = 1) -> list:
    related_df = get_related_code(ticker_symbol)
    realted_result = []
    temp_dict = {
        0: "(未入池)",
        1: "",
        2: "(未上架)",
        3: "(未代销)",
    }
    for _, val in related_df.iterrows():
        flag = check_in_tranche(val["init_fund_code"])
        if flag not in (0, 1):
            pass
        else:
            if (flag != 1) and (include_out_tranche != 1):
                pass
            else:
                realted_result.append(f'{val["init_fund_code"]}{temp_dict[flag]}')

        if val["linked_code"] is not None:
            flag2 = check_in_tranche(val["linked_code"])
            if flag2 not in (0, 1):
                pass
            else:
                if (flag2 != 1) and (include_out_tranche != 1):
                    pass
                else:
                    # print(flag2 != 1, flag2 != 1, (flag2 != 1) and (include_out_tranche != 1))
                    realted_result.append(f'{val["linked_code"]}{temp_dict[flag2]}')
    realted_result = list(set(realted_result))
    realted_result.sort()
    return realted_result


def get_fund_name(ticker_symbol: str) -> str:
    query_sql = f"""
        SELECT
            SEC_SHORT_NAME 
        FROM
            fund_info 
        WHERE
            1 = 1 
            AND EXPIRE_DATE IS NULL 
            AND TICKER_SYMBOL = '{ticker_symbol}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["SEC_SHORT_NAME"].values[0]


def get_fund_tranche(ticker_symbol: str) -> str:
    query_sql = f"""
    SELECT
        TRANCHE 
    FROM
        portfolio_basic_products 
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = '{ticker_symbol}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["TRANCHE"].values[0]


def get_tracking_error(ticker_symbol: str, end_date: str) -> float:
    query_sql = f"""
        SELECT 
            START_DATE, 
            END_DATE 
        FROM 
            portfolio_dates 
        WHERE 
            1 = 1 
            AND PORTFOLIO_NAME = 'ALL' 
            AND END_DATE = '{end_date}' AND DATE_NAME = '近1年' 
    """

    dates = DB_CONN_JJTG_DATA.exec_query(query_sql)
    try:
        start_date = dates["START_DATE"].values[0]
        end_date = dates["END_DATE"].values[0]
    except:
        raise ValueError(f"{end_date}没有对应的交易日")
    query_sql = f"""
    SELECT
        std(a.Alpha) * SQRT( 250 ) AS TRACKING_ERROR 
    FROM
        (
        SELECT
            b.SecuCode,
            c.EndDate,
            ( d.NVRDailyGrowthRate - c.DailyBenchGR )/ 100 AS Alpha 
        FROM
            secumainall b
            JOIN mf_benchmarkgrtrans c ON c.InnerCode = b.InnerCode
            JOIN mf_netvalueretranstwo d ON d.InnerCode = b.InnerCode 
            AND d.TradingDay = c.EndDate 
        WHERE
            1 = 1 
            AND b.SecuCode = '{ticker_symbol}' 
            AND c.EndDate BETWEEN '{start_date}' 
        AND '{end_date}' 
        ) a
    """
    result = DB_CONN_JY.exec_query(query_sql)
    if result.empty:
        raise ValueError(f"{ticker_symbol}没有跟踪误差")
    else:
        return result["TRACKING_ERROR"].values[0]


def get_etf_fund_code(ticker_symbol: str) -> str:
    query_sql = f"""
    SELECT
        b.SecuCode
    FROM
        mf_coderelationshipnew a
        JOIN secumainall b ON b.InnerCode = a.InnerCode
        JOIN secumainall c ON c.InnerCode = a.RelatedInnerCode 
    WHERE
        1 = 1 
        AND a.CodeDefine = 24 
        AND a.IfEffected = 1
        and c.SecuCode = '{ticker_symbol}'
    """
    result = DB_CONN_JY_LOCAL.exec_query(query_sql)
    if result.empty:
        raise ValueError(f"{ticker_symbol}没有对应的ETF基金代码")
    else:
        return result["SecuCode"].values[0]


def get_fund_nav_benchmark(
    ticker_symbol: str, start_date: str, end_date: str
) -> pd.DataFrame:
    query_sql = f"""
    SELECT
            b.SecuCode as TICKER_SYMBOL,
            DATE_FORMAT(c.EndDate, "%Y%m%d") as TRADE_DT,
            d.NVRDailyGrowthRate/100 as FUND_RET,
            c.DailyBenchGR/100 as BENCH_RET
        FROM
            secumainall b
            JOIN mf_benchmarkgrtrans c ON c.InnerCode = b.InnerCode
            JOIN mf_netvalueretranstwo d ON d.InnerCode = b.InnerCode 
            AND d.TradingDay = c.EndDate 
        WHERE
            1 = 1 
            AND b.SecuCode = '{ticker_symbol}' 
            AND c.EndDate BETWEEN '{start_date}' AND '{end_date}'     
    """
    ret_df = DB_CONN_JY.exec_query(query_sql)
    ret_df["FUND_RET"] = ret_df["FUND_RET"].astype("float")
    ret_df["BENCH_RET"] = ret_df["BENCH_RET"].astype("float")
    ret_df["ADJUST_NAV"] = (1 + ret_df["FUND_RET"]).cumprod()
    ret_df["S_DQ_CLOSE"] = (1 + ret_df["BENCH_RET"]).cumprod()
    return ret_df[["TICKER_SYMBOL", "TRADE_DT", "ADJUST_NAV", "S_DQ_CLOSE"]]


def get_total_fee(ticker_symbol: str) -> float:
    query_sql = f"""
    SELECT
        TOTAL_FEE 
    FROM
        `view_fund_fee` 
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = '{ticker_symbol}'    
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["TOTAL_FEE"].values[0]


def get_fund_2y_rank(ticker_symbol, end_date):
    query_sql = f"""
        SELECT
        TRADE_DT,
        TICKER_SYMBOL,
        `LEVEL`,
        F_AVGRETURN_TWOYEAR
    FROM
        fund_performance_rank_pct
    WHERE
        1 = 1 
        AND TICKER_SYMBOL = '{ticker_symbol}' 
        AND TRADE_DT = '{end_date}' 
        AND `LEVEL` = 'LEVEL_1'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["F_AVGRETURN_TWOYEAR"].values[0]


def get_fund_name_dict(ticker_symbol: str) -> dict:
    query_sql = f"""
    SELECT
        CASE
            InfoType 
            WHEN 4 THEN 
            '简称' ELSE '全称' 
        END AS InfoType,
        DisclName
    FROM
        mf_fundprodname a
        JOIN mf_fundarchives b ON a.InnerCode = b.InnerCode 
    WHERE
        1 = 1 
        AND b.SecuCode = '{ticker_symbol}' 
        AND a.IfEffected = 1 
        AND a.InfoType IN (4, 8)
    """
    df = DB_CONN_JY.exec_query(query_sql)
    fund_name_dict = dict(zip(df["InfoType"], df["DisclName"]))
    return fund_name_dict


def check_fund_manager_change(ticker_symbol: str, end_date: str) -> int:
    query_sql = f"""
    SELECT
        a.TICKER_SYMBOL,
        max(
            CASE
                ifnull( a.DIMISSION_DATE, '20991231' ) 
                BETWEEN DATE_SUB( '{end_date}', INTERVAL 1 YEAR ) 
                AND '{end_date}' 
                WHEN 1 THEN 1 ELSE 0 
            END 
        ) AS 'IF_MANAGER_CHANGE' 
        FROM
            fund_manager_info a
            JOIN fund_info b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND a.POSITION = 'FM' 
            AND ifnull( b.EXPIRE_DATE, '20991231' ) >= '{end_date}' 
            AND b.TICKER_SYMBOL = '{ticker_symbol}'
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["IF_MANAGER_CHANGE"].values[0]


if __name__ == "__main__":
    print(get_related_code_str("007939", 1))
