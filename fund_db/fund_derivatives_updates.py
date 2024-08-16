from itertools import combinations

import pandas as pd

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_JY_LOCAL

DB_CONN = DB_CONN_JJTG_DATA


def update_fund_derivatives_stock_holdings(report_date: str, if_10: bool = 0):
    """
    更新基金持仓衍生表

    Parameters
    ----------
    report_date : str
        报告期
    if_10 : bool, optional
        是否前十大持仓，否则为全部持仓, by default 0
    """
    if_top_10_name = {0: "ALL", 1: "前十大"}

    if_top_10_3 = {1: "_q", 0: ""}
    query_sql = f"""
        WITH t1 AS ( 
            SELECT DISTINCT fund_id, report_date 
            FROM fund_holdings{if_top_10_3[if_10]}
            WHERE report_date = '{report_date}' 
            AND SECURITY_TYPE = 'E'
        ) SELECT
        a.REPORT_DATE,
        a.HOLDING_TICKER_SYMBOL AS TICKER_SYMBOL,
        '{if_top_10_name[if_10]}'  AS HOLDING_TYPE,
        c.LEVEL_1,
        c.LEVEL_2,
        c.LEVEL_3,
        b.MANAGEMENT_COMPANY_NAME,
        count( a.HOLDING_TICKER_SYMBOL ) AS NUMBER,
        sum( a.MARKET_VALUE ) AS MARKET_VALUE 
        FROM
            t1
            join fund_holdings{if_top_10_3[if_10]} a on t1.FUND_ID = a.FUND_ID and a.REPORT_DATE = t1.REPORT_DATE
            JOIN fund_info b ON b.FUND_ID = a.FUND_ID
            JOIN fund_type_own c ON c.TICKER_SYMBOL = b.TICKER_SYMBOL 
            AND c.REPORT_DATE = a.REPORT_DATE 
        WHERE
            1 = 1 
            AND a.SECURITY_TYPE = 'E' 
            and b.is_main = 1
        GROUP BY
            a.REPORT_DATE,
            a.HOLDING_TICKER_SYMBOL,
            c.LEVEL_1,
            c.LEVEL_2,
            c.LEVEL_3,
            b.MANAGEMENT_COMPANY_NAME
    """
    print(query_sql)
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    if df.empty:
        return pd.DataFrame()

    industry = dm.get_stock_sw_industry_21(report_date)
    df_temp = df.merge(industry)

    groupby_col = [
        "REPORT_DATE",
        "HOLDING_TYPE",
        "TICKER_SYMBOL",
    ]

    groupby_rolling = [
        "LEVEL_1",
        "MANAGEMENT_COMPANY_NAME",
        "INDUSTRY_NAME",
    ]
    #
    sub_level = [
        "LEVEL_2",
        "LEVEL_3",
    ]
    needed_group_col = sum(
        (
            list(map(list, combinations(groupby_rolling, i)))
            for i in range(len(groupby_rolling) + 1)
        ),
        [],
    )
    # 如果有一级行业再增加二级、三级行业分类
    temp_needed_group_col = []
    for i in needed_group_col:
        temp_needed_group_col.append(i)
        if "LEVEL_1" in i:
            temp_needed_group_col.extend((i + ["LEVEL_2"], i + ["LEVEL_2", "LEVEL_3"]))
    # 分组增加二级、三级
    groupby_rolling += sub_level
    result_list = []
    for col in temp_needed_group_col:
        temp = df_temp.groupby(groupby_col + col).sum().reset_index()
        if col_filled := list(set(groupby_rolling) - set(col)):
            temp[col_filled] = "ALL"
        result_list.append(temp)
    result = pd.concat(result_list)
    result[["NUMBER_SCORE", "MARKET_VALUE_SCORE"]] = round(
        result.groupby(groupby_rolling)[["NUMBER", "MARKET_VALUE"]].rank(pct=True)
        * 100,
        4,
    )
    DB_CONN.upsert(result, table="fund_derivatives_stock_holdings")


def update_fund_derivatives_crowd(report_date: str, if_10: bool = 0):
    sql_dict1 = {1: "and holding_num <= 10", 0: ""}
    if_top_10_3 = {1: "_q", 0: ""}
    query_sql = f"""
    SELECT 
        REPORT_DATE, 
        FUND_ID, 
        HOLDING_TICKER_SYMBOL as TICKER_SYMBOL,
        RATIO_IN_NA 
    FROM 
        fund_holdings{if_top_10_3[if_10]}
    WHERE 
        REPORT_DATE = '{report_date}' 
        AND SECURITY_TYPE = 'E' 
        {sql_dict1[if_10]}
    """
    df_holdings = DB_CONN.exec_query(query_sql)

    sql_dict2 = {1: "前十大", 0: "ALL"}

    query_sql2 = f"""
    SELECT
        REPORT_DATE,
        TICKER_SYMBOL,
        HOLDING_TYPE,
        MARKET_VALUE_SCORE,
        NUMBER_SCORE
    FROM
        fund_derivatives_stock_holdings
    WHERE
        1=1
        and `HOLDING_TYPE` = '{sql_dict2[if_10]}'
        AND `LEVEL_1` = '主动权益' 
        AND `LEVEL_2` = 'ALL' 
        AND `LEVEL_3` = 'ALL' 
        AND `MANAGEMENT_COMPANY_NAME` = 'ALL' 
        AND `INDUSTRY_NAME` = 'ALL' 
        AND `REPORT_DATE` = '{report_date}' 
    """

    df = DB_CONN.exec_query(query_sql2)
    df_temp = df_holdings.merge(
        df,
        left_on=["REPORT_DATE", "TICKER_SYMBOL"],
        right_on=["REPORT_DATE", "TICKER_SYMBOL"],
    )

    df_temp["TOTAL_VALUE_SCORE"] = (
        df_temp["MARKET_VALUE_SCORE"] * df_temp["RATIO_IN_NA"] / 100
    )
    df_temp["NUMBER_SCORE"] = df_temp["NUMBER_SCORE"] * df_temp["RATIO_IN_NA"] / 100

    df_temp = df_temp.groupby(by=["REPORT_DATE", "FUND_ID", "HOLDING_TYPE"])[
        ["TOTAL_VALUE_SCORE", "NUMBER_SCORE", "RATIO_IN_NA"]
    ].sum()

    try:
        df_temp["TOTAL_VALUE_SCORE"] = (
            100 * df_temp["TOTAL_VALUE_SCORE"] / df_temp["RATIO_IN_NA"]
        )
        df_temp["NUMBER_SCORE"] = 100 * df_temp["NUMBER_SCORE"] / df_temp["RATIO_IN_NA"]
        df_temp.drop(columns="RATIO_IN_NA", inplace=True)
        df_temp["CROWD_TYPE"] = "ALL"
        df_temp = df_temp.reset_index()

        DB_CONN_JJTG_DATA.upsert(df_temp, table="fund_derivatives_crowding")
    except Exception as e:
        print(e)


def update_fund_duration(report_date: str):
    """
    更新基金久期

    Parameters
    ----------
    report_date : str
        报告期
    """
    query_sql = f"""
    SELECT
        a.EndDate AS REPORT_DATE,
        e.SecuCode AS TICKER_SYMBOL,
        avg( abs( NetValueEnd / ChangeRange * 100 / b.NV ) ) AS DURATION,
        avg( b.TotalAsset / NV ) AS LEVERAGE 
    FROM
        `mf_risksecurityanalysis` a
        JOIN mf_assetallocationall b ON a.InnerCode = b.InnerCode 
        AND a.EndDate = b.EndDate
        JOIN mf_fundarchives d ON d.InnerCode = a.InnerCode
        JOIN mf_fundarchives e ON e.MainCode = d.SecuCode 
    WHERE
        1 = 1 
        AND VariableType = 10 
        AND a.EndDate = '{report_date}' 
        AND a.Mark = 2 
    GROUP BY
        e.SecuCode,
        a.EndDate
    """
    df = DB_CONN_JY_LOCAL.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="fund_style_bond")


for year in range(2000, 2025):
    for month in ["0331", "0630", "0930", "1231"]:
        report_date = str(year) + month
        # update_fund_derivatives_stock_holdings(report_date=report_date, if_10=1)
        # update_fund_derivatives_crowd(report_date=report_date, if_10=1)
        # update_fund_derivatives_stock_holdings(report_date=report_date, if_10=0)
        # update_fund_derivatives_crowd(report_date=report_date, if_10=0)
        update_fund_duration(report_date=report_date)
    # for month in ["0630", "1231"]:
    #     report_date = str(year) + month
    #     update_fund_derivatives_stock_holdings(report_date=report_date, if_10=0)
    #     update_fund_derivatives_stock_holdings(report_date=report_date, if_10=1)
    #     update_fund_derivatives_crowd(report_date=report_date, if_10=0)
    #     update_fund_derivatives_crowd(report_date=report_date, if_10=1)
