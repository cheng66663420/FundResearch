# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_WIND

DB_CONN = DB_CONN_JJTG_DATA

# 万得数据库取股票质量数据


def get_quality_component(report_date):
    sql = f"""
        SELECT
            b.S_INFO_CODE AS TICKER_SYMBOL,
            a.EST_DT,
            a.EST_ROE,
            c.S_FA_ROE_TTM AS ROE_TTM,
            c.S_FA_ROA_TTM AS ROA_TTM 
        FROM
            AShareConsensusRollingData a
            JOIN AShareDescription b ON a.S_INFO_WINDCODE = b.S_INFO_WINDCODE
            JOIN AShareTTMHis c ON b.S_INFO_COMPCODE = c.S_INFO_COMPCODE 
            AND a.EST_DT = c.REPORT_PERIOD 
        WHERE
            1 = 1 
            AND a.ROLLING_TYPE = 'FY1' 
            AND a.EST_DT = '{report_date}'
    """
    return DB_CONN_WIND.exec_query(sql)


# 本地库取全部基金
def fund_stock_holdings(date: str):
    sql = f"""
        SELECT
            a.REPORT_DATE,
            a.TICKER_SYMBOL as FUND_SYMBOL,
            c.HOLDING_TICKER_SYMBOL,
            c.HOLDING_SEC_SHORT_NAME,
            c.RATIO_IN_NA
        FROM
            `fund_type_own` a 
            JOIN fund_info b 
            on a.TICKER_SYMBOL = b.TICKER_SYMBOL
            JOIN fund_holdings c 
            on b. FUND_ID = c.FUND_ID and a.report_date = c.report_date
        WHERE
            a.REPORT_DATE = '{date}'
            AND c.SECURITY_TYPE='E'
    """
    return DB_CONN.exec_query(sql)


def cal_quality_score(report_date):
    # 取出质量得分
    df = get_quality_component(report_date)
    df["EST_ROE_SCORE"] = df["EST_ROE"].rank(pct=True) * 100
    df["ROE_TTM_SCORE"] = df["ROE_TTM"].rank(pct=True) * 100
    df["ROA_TTM_SCORE"] = df["ROA_TTM"].rank(pct=True) * 100
    df["TOTAL_SCORE"] = np.mean(df.iloc[:, 6:8], axis=1)

    # 取出目标基金所持有的全部股票和比例
    fund_stock_holding = fund_stock_holdings(report_date)

    # 并表
    df_quality = pd.merge(
        fund_stock_holding,
        df,
        left_on="HOLDING_TICKER_SYMBOL",
        right_on="TICKER_SYMBOL",
        how="left",
    )

    # 基金质量得分
    df_quality["RATIO_SCORE"] = df_quality["RATIO_IN_NA"] * df_quality["TOTAL_SCORE"]
    result = (
        df_quality.groupby(by=["FUND_SYMBOL", "REPORT_DATE"])["RATIO_SCORE"].sum() / 100
    )
    result = result.reset_index()
    result.columns = ["TICKER_SYMBOL", "REPORT_DATE", "QUALITY"]
    return result


def cal_top10_concentration(report_date: str) -> pd.DataFrame():
    """
    计算基金前十大集中度

    Parameters
    ----------
    report_date : str
        报告期

    Returns
    -------
    pd.DataFrame
        columns = [TICKER_SYMBOL, REPORT_DATE, TOP10_CONCENTRATION]
    """
    query_sql = f"""
    SELECT
        b.TICKER_SYMBOL,
        a.REPORT_DATE,
        sum( a.RATIO_IN_NA ) AS TOP10_CONCENTRATION 
    FROM
        fund_holdings_q a
        JOIN fund_info b ON a.fund_id = b.fund_id 
    WHERE
        1 = 1 
        AND SECURITY_TYPE = 'E' 
        AND HOLDING_NUM <= 10 
        AND REPORT_DATE = '{report_date}' 
    GROUP BY
        b.TICKER_SYMBOL,
        a.REPORT_DATE
    """
    return DB_CONN.exec_query(query_sql)


def cal_growth_value(report_date: str) -> pd.DataFrame:
    """
    计算基金前十大集中度

    Parameters
    ----------
    report_date : str
        报告期

    Returns
    -------
    pd.DataFrame
        columns = [TICKER_SYMBOL, REPORT_DATE, TOP10_CONCENTRATION]
    """
    trade_dt = dm.offset_trade_dt(report_date, n_days_before=0)
    query_sql = f"""
    WITH a AS (
        SELECT
        CASE
                
            WHEN MONTH
                ( END_DATE ) = 6 THEN
                    CONCAT( YEAR ( END_DATE ), 0, MONTH ( END_DATE ), '30' ) ELSE CONCAT( YEAR ( END_DATE ), MONTH ( END_DATE ), '31' ) 
                    END AS REPORT_DATE,
                TICKER_SYMBOL,
                RAW_X AS GROWTH_VALUE,
                RAW_Y AS SMALL_LARGE
            FROM
                fund_style_invest 
            WHERE
                1 = 1 
                AND END_DATE = '{trade_dt}' 
            ) SELECT DATE
            ( REPORT_DATE ) AS REPORT_DATE,
            TICKER_SYMBOL,
            GROWTH_VALUE,
            SMALL_LARGE
    FROM
        a
    """
    return DB_CONN.exec_query(query_sql)


def cal_fund_turnover(report_date: str) -> pd.DataFrame:
    """
    计算基金换手率

    Parameters
    ----------
    report_date : str
        报告期

    Returns
    -------
    pd.DataFrame
        columns=[REPORT_DATE, TICKER_SYMBOL, NET_ASSET_ESTIMATED, TURNOVER_RATE]
    """
    query_sql = f"""
    WITH a AS (
        SELECT
            a.EndDate as END_DATE,
            a.TICKER_SYMBOL,
            round( a.TrusteeExpense / b.MAX_CHAR_RATE * 100, 4 ) AS 'NET_ASSET_ESTIMATED' 
        FROM
            fund_income_statement a
            JOIN fund_fee_new b ON b.TICKER_SYMBOL = a.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND b.CHARGE_TYPE_CN = '托管费' 
            AND b.IS_EXE = 1 
            AND a.Mark = 1 
        ),
        b AS (
        SELECT
            a.fund_id,
            b.TICKER_SYMBOL,
            a.REPORT_DATE,
            a.SELL_INCOME,
            a.BUY_COST 
        FROM
            fund_equ_trade a
            RIGHT JOIN fund_info b ON a.FUND_ID = b.FUND_ID 
        ) SELECT
        b.REPORT_DATE,
        c.TICKER_SYMBOL,
        a.NET_ASSET_ESTIMATED,
        round(( b.BUY_COST + b.SELL_INCOME )/ a.NET_ASSET_ESTIMATED * 100, 4 ) AS TURNOVER_RATE 
    FROM
        a
        JOIN b ON a.END_DATE = b.REPORT_DATE 
        AND a.TICKER_SYMBOL = b.TICKER_SYMBOL
        JOIN fund_info c ON c.fund_id = b.fund_id
        JOIN fund_type_own d ON d.ticker_symbol = a.TICKER_SYMBOL 
        AND d.report_date = b.REPORT_DATE 
    WHERE
        1 = 1 
        AND a.END_DATE = '{report_date}'
    ORDER BY
        REPORT_DATE,
        TICKER_SYMBOL
    """
    return DB_CONN.exec_query(query_sql)


def cal_fund_sector_rotation(report_date: str) -> pd.DataFrame:
    """
    计算基金板块轮动

    Parameters
    ----------
    report_date : str
        报告期

    Returns
    -------
    pd.DataFrame
        columns=[
            REPORT_DATE, TICKER_SYMBOL,
            CHANGE_SCORE, MAX_SECTOR_CHANGE, SECTOR_ROTATION
        ]
    """
    start_date = dm.offset_period_dt(report_date, n=-2, period="y")
    query_sql = f"""
    WITH t AS (
        SELECT
            TICKER_SYMBOL,
            SECTOR,
            REPORT_DATE,
            SECTOR_RATIO_IN_EQUITY,
            ABS(
            SECTOR_RATIO_IN_EQUITY - LAG( SECTOR_RATIO_IN_EQUITY ) OVER ( PARTITION BY TICKER_SYMBOL, SECTOR ORDER BY REPORT_DATE )) AS RATIO_CHANGE 
        FROM
            fund_holding_sector 
        WHERE
            1 = 1 
            AND REPORT_DATE >= '{start_date}'
            AND REPORT_DATE <= "{report_date}"
        ORDER BY
            SECTOR 
        ),
        a AS (
        SELECT
            t.*,
            avg( SECTOR_RATIO_IN_EQUITY ) over ( PARTITION BY t.TICKER_SYMBOL, t.SECTOR ORDER BY t.REPORT_DATE rows 2 preceding ) AS RATIO_AVG,
            round(
                AVG( t.RATIO_CHANGE ) over ( PARTITION BY t.TICKER_SYMBOL, t.SECTOR ORDER BY t.REPORT_DATE rows 2 preceding ),
                6 
            ) AS RATIO_CHANGE_AVG 
        FROM
            t 
        ),
        t1 AS (SELECT
        a.TICKER_SYMBOL,
        a.REPORT_DATE,
        round( sum( a.RATIO_AVG * RATIO_CHANGE_AVG )/ sum( a.RATIO_AVG ), 4 ) AS CHANGE_SCORE,
        round(
            max( RATIO_AVG ) * (
            100- round( sum( a.RATIO_AVG * RATIO_CHANGE_AVG )/ sum( a.RATIO_AVG ), 4 ))/ 100,
            4 
        ) AS MAX_SECTOR_CHANGE 
    FROM
        a 
    GROUP BY
        a.ticker_symbol,
        a.report_date
        ) SELECT
        *,
        CASE
            WHEN CHANGE_SCORE >= 20 
            AND MAX_SECTOR_CHANGE <= 50 THEN '板块轮动' WHEN MAX_SECTOR_CHANGE > 50 THEN
                '赛道' 
                WHEN CHANGE_SCORE < 20 
                AND MAX_SECTOR_CHANGE <= 50 THEN
                    '不轮动' 
                    END AS SECTOR_ROTATION 
            FROM
                t1 
            WHERE
                CHANGE_SCORE IS NOT NULL 
                AND REPORT_DATE = "{report_date}"
            ORDER BY
                REPORT_DATE,
                TICKER_SYMBOL
    """
    return DB_CONN.exec_query(query_sql)


def cal_top3_industry_concentration(report_date: str) -> pd.DataFrame:
    """
    计算前三大行业占比
    Parameters
    ----------
    report_date : str
        报告期

    Returns
    -------
    pd.DataFrame
        columns=[
            REPORT_DATE, TICKER_SYMBOL,
            CHANGE_SCORE, MAX_SECTOR_CHANGE, SECTOR_ROTATION
        ]
    """
    query_sql = f"""
        WITH t AS (
            SELECT
                REPORT_DATE,
                TICKER_SYMBOL,
                INDUSTRY_RATIO_IN_EQUITY,
                RANK() over ( PARTITION BY REPORT_DATE, TICKER_SYMBOL ORDER BY INDUSTRY_RATIO_IN_EQUITY DESC ) AS ranks 
            FROM
                fund_holding_industry 
               
            WHERE
                1 = 1 
                AND REPORT_DATE = "{report_date}"
                 and level_num = 1
            ORDER BY
                TICKER_SYMBOL,
                REPORT_DATE,
                INDUSTRY_RATIO_IN_EQUITY DESC 
            ) SELECT
            t.REPORT_DATE,
            t.TICKER_SYMBOL,
            SUM( INDUSTRY_RATIO_IN_EQUITY ) AS INDUSTRY_CONCENTRATION
        FROM
            t 
        WHERE
            t.ranks <= 3 
        GROUP BY
            REPORT_DATE,
            TICKER_SYMBOL 
        ORDER BY
            t.REPORT_DATE,
            t.TICKER_SYMBOL
    """
    return DB_CONN.exec_query(query_sql)


def cal_crowd_score(report_date: str) -> pd.DataFrame:
    """
    计算前三大行业占比
    Parameters
    ----------
    report_date : str
        报告期

    Returns
    -------
    pd.DataFrame
        columns=[
            REPORT_DATE, TICKER_SYMBOL, CROWD_SCORE
        ]
    """
    query_sql = f"""
    SELECT
        a.REPORT_DATE,
        b.TICKER_SYMBOL,
        a.TOTAL_VALUE_SCORE as CROWD_SCORE
    FROM
        fund_derivatives_crowding a
        JOIN fund_info b ON b.FUND_ID = a.FUND_ID 
    WHERE
        1=1
        and `REPORT_DATE` = '{report_date}' 
        AND `CROWD_TYPE` = 'ALL' 
        AND `HOLDING_TYPE` = 'ALL' 
    """
    return DB_CONN.exec_query(query_sql)


def cal_prac_inst_hold_ratio(report_date: str) -> pd.DataFrame:
    """
    计算基金从业人员机构人员持有比例

    Parameters
    ----------
    report_date : str
        报告期
    Returns
    -------
    pd.DataFrame
        基金从业人员-机构人员持有比例DataFrame
    """
    query_sql = f"""
    SELECT
        b.TICKER_SYMBOL,
        a.REPORT_DATE,
        a.PRAC_HOLD_RATIO,
        a.INST_HOLD_RATIO
    FROM
        fund_holder_info a
        JOIN fund_info b ON b.FUND_ID = a.FUND_ID 
    WHERE
        1 = 1 
        AND ( a.INFO_SOURCE = '年度报告' OR a.INFO_SOURCE = '中期报告' ) 
        AND a.PRAC_HOLD_RATIO IS NOT NULL 
        AND a.REPORT_DATE = '{report_date}'
    ORDER BY
        a.REPORT_DATE,
        b.TICKER_SYMBOL
    """
    return DB_CONN.exec_query(query_sql)


def cal_fund_style(report_date: str) -> pd.DataFrame:
    """
    计算基金的风格特征，包括成长均衡价值、大盘中盘小盘
    Parameters
    ----------
    report_date : str
        报告期
    """
    fund_style = DB_CONN_JJTG_DATA.exec_query(
        f"""
        WITH t1 AS (
            SELECT
                a.REPORT_DATE,
            CASE
                    b.IF_TRADING_DAY 
                    WHEN 1 THEN
                    b.TRADE_DT ELSE b.PREV_TRADE_DATE 
                END AS TRADE_DT 
            FROM
                md_report_date_calender a
                JOIN md_tradingdaynew b ON a.REPORT_DATE = b.TRADE_DT 
            WHERE
                1 = 1 
                AND b.SECU_MARKET = 83 
                AND a.IF_SEMI_YEAR = 1 
            ) SELECT
            t1.TRADE_DT,
            t1.REPORT_DATE,
            a.TICKER_SYMBOL as HOLDING_TICKER_SYMBOL,
            a.SIZE,
            a.GROWTH 
        FROM
            t1
            JOIN dy1d_exposure_cne6_sw21 a ON t1.TRADE_DT = a.TRADE_DT
        WHERE t1.REPORT_DATE = '{report_date}' 
    """
    )

    fund_holdings = DB_CONN_JJTG_DATA.exec_query(
        f"""                                                                                           
    SELECT
    	a.REPORT_DATE,
        b.TICKER_SYMBOL AS TICKER_SYMBOL,
        a.HOLDING_TICKER_SYMBOL,  
        a.RATIO_IN_EQUITY   
    FROM
        fund_holdings a
    JOIN 
        fund_info b ON a.FUND_ID = b.FUND_ID
    WHERE 
        REPORT_DATE = '{report_date}'  
        AND SECURITY_TYPE = 'E' """
    )
    fund_holdings_style = fund_holdings.merge(
        fund_style, on=["HOLDING_TICKER_SYMBOL", "REPORT_DATE"], how="inner"
    )
    fund_holdings_style["TOTAL_SIZE"] = (
        fund_holdings_style["RATIO_IN_EQUITY"] * fund_holdings_style["SIZE"]
    )
    fund_holdings_style["TOTAL_GROWTH"] = (
        fund_holdings_style["RATIO_IN_EQUITY"] * fund_holdings_style["GROWTH"]
    )
    fund_holdings_style = (
        fund_holdings_style.groupby(["REPORT_DATE", "TICKER_SYMBOL"])
        .agg({"SIZE": np.sum, "GROWTH": np.sum})
        .rename(columns={"SIZE": "SMALL_LARGE", "GROWTH": "GROWTH_VALUE"})
    )
    return fund_holdings_style.reset_index()


def fund_barra_style(report_date: str) -> pd.DataFrame:
    fund_style = DB_CONN_JJTG_DATA.exec_query(
        f"""
    WITH t1 AS (
        SELECT
            a.REPORT_DATE,
        CASE
                b.IF_TRADING_DAY 
                WHEN 1 THEN
                b.TRADE_DT ELSE b.PREV_TRADE_DATE 
            END AS TRADE_DT 
        FROM
            md_report_date_calender a
            JOIN md_tradingdaynew b ON a.REPORT_DATE = b.TRADE_DT 
        WHERE
            1 = 1 
            AND b.SECU_MARKET = 83 
            AND a.IF_SEMI_YEAR = 1 
        ) SELECT
        t1.REPORT_DATE,
        a.TICKER_SYMBOL AS HOLDING_TICKER_SYMBOL,
        a.BETA,
        a.MOMENTUM,
        a.SIZE,
        a.EARNYILD,
        a.RESVOL,
        a.GROWTH,
        a.BTOP,
        a.LEVERAGE,
        a.LIQUIDTY,
        a.MIDCAP,
        a.DIVYILD,
        a.EARNQLTY,
        a.EARNVAR,
        a.INVSQLTY,
        a.LTREVRSL,
        a.PROFIT,
        a.ANALSENTI,
        a.INDMOM,
        a.SEASON,
        a.STREVRSL,
        a.Agriculture,
        a.Automobiles,
        a.Banks,
        a.BuildMater,
        a.Chemicals,
        a.Commerce,
        a.Computers,
        a.Conglomerates,
        a.ConstrDecor,
        a.Defense,
        a.ElectricalEquip,
        a.Electronics,
        a.FoodBeverages,
        a.HealthCare,
        a.HomeAppliances,
        a.Leisure,
        a.LightIndustry,
        a.MachineEquip,
        a.Media,
        a.Mining,
        a.NonbankFinan,
        a.NonferrousMetals,
        a.RealEstate,
        a.Steel,
        a.Telecoms,
        a.TextileGarment,
        a.Transportation,
        a.Utilities,
        a.BasicChemicals,
        a.BeautyCare,
        a.Coal,
        a.EnvironProtect,
        a.Petroleum,
        a.PowerEquip,
        a.RetailTrade,
        a.SocialServices,
        a.TextileApparel,
        a.COUNTRY 
    FROM
        t1
        JOIN dy1d_exposure_cne6_sw21 a ON t1.TRADE_DT = a.TRADE_DT 
    WHERE
        t1.REPORT_DATE = '{report_date}' 
    """
    )
    cols = fund_style.columns[2:].tolist()
    fund_holdings = DB_CONN_JJTG_DATA.exec_query(
        f"""                                                                                           
    SELECT
    	a.REPORT_DATE,
        b.TICKER_SYMBOL AS TICKER_SYMBOL,
        a.HOLDING_TICKER_SYMBOL,  
        a.RATIO_IN_EQUITY   
    FROM
        fund_holdings a
    JOIN 
        fund_info b ON a.FUND_ID = b.FUND_ID
    WHERE 
        REPORT_DATE = '{report_date}'  
        AND SECURITY_TYPE = 'E' """
    )
    fund_holdings_style = fund_holdings.merge(
        fund_style, on=["HOLDING_TICKER_SYMBOL", "REPORT_DATE"], how="left"
    )

    for col in cols:
        fund_holdings_style[col] = (
            fund_holdings_style["RATIO_IN_EQUITY"] * fund_holdings_style[col]
        )

    fund_holdings_barra_style = fund_holdings_style[
        ["REPORT_DATE", "TICKER_SYMBOL"] + cols
    ]
    return (
        fund_holdings_barra_style.groupby(["REPORT_DATE", "TICKER_SYMBOL"])
        .sum()
        .reset_index()
    )


def main(report_date: str):
    func_list = [
        cal_quality_score,
        cal_top10_concentration,
        cal_growth_value,
        cal_fund_turnover,
        cal_fund_sector_rotation,
        cal_top3_industry_concentration,
        cal_crowd_score,
        cal_prac_inst_hold_ratio,
        cal_fund_style,
    ]
    for func in func_list:
        print(func.__name__)
        result_df = func(report_date)
        result_df.rename(columns={"REPORT_DATE": "END_DATE"}, inplace=True)
        result_df["PUBLISH_DATE"] = result_df["END_DATE"].apply(
            lambda s: s + relativedelta(months=3)
        )
        DB_CONN_JJTG_DATA.upsert(result_df, table="fund_derivatives_labels_stock")
    # result_list = [func(report_date) for func in func_list]
    # result_df = reduce(
    #     lambda x, y: pd.merge(x, y, how="outer", on=["TICKER_SYMBOL", "REPORT_DATE"]),
    #     result_list
    # )
    # print(result_df)
    # result_df.rename(columns={"REPORT_DATE": "END_DATE"}, inplace=True)
    # result_df["PUBLISH_DATE"] = result_df["END_DATE"].apply(
    #     lambda s: s + relativedelta(months=3)
    # )


if __name__ == "__main__":
    for year in range(2023, 2024):
        for date in ["1231"]:
            print(str(year) + date)
            result_df = fund_barra_style(str(year) + date)
            DB_CONN_JJTG_DATA.upsert(
                result_df, table="fund_derivatives_holdings_barra_exposure"
            )
            main(str(year) + date)
