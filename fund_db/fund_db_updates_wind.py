import quant_utils.data_moudle as dm
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_WIND


def update_ashareindustriescode(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    select * from ashareindustriescode where OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="ashareindustriescode")


def update_chinamutualfundsector(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    select 
        * 
    from 
        chinamutualfundsector 
    where 
        1=1
        and OPDATE >= '{update_date}'
        and S_INFO_SECTORENTRYDT is not NULL
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="chinamutualfundsector")


def update_chinamutualfundbenchmark(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    select 
        * 
    from 
        chinamutualfundbenchmark 
    where 
        1=1
        and OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="chinamutualfundbenchmark")


def update_chinamutualfundnav(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        *
    FROM
        chinamutualfundnav 
    WHERE
        1 = 1 
        and OPDATE >= date('{update_date}')
        and OPDATE < DATE_ADD('{update_date}',INTERVAL 3 DAY)
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="chinamutualfundnav")


def update_global_index_eod(update_date: str = None):
    """
    更新全球股票指数的行情

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT

    query_sql = f"""
    SELECT
        S_INFO_WINDCODE,
        TRADE_DT,
        S_DQ_CLOSE,
        S_DQ_OPEN,
        S_DQ_HIGH,
        S_DQ_LOW,
        S_DQ_PCTCHANGE,
        S_DQ_AMOUNT,
        S_DQ_VOLUME,
        SUBSTRING_INDEX( S_INFO_WINDCODE, '.', 1 ) AS TICKER_SYMBOL,
        ROUND( LOG( S_DQ_PCTCHANGE / 100+1 )* 100, 4 ) AS LOG_RET,
        OPDATE as UPDATE_TIME
    FROM
        hkindexeodprices 
    WHERE
        1=1
        and OPDATE >= date('{update_date}')
        and OPDATE < DATE_ADD('{update_date}',INTERVAL 3 DAY) UNION
    SELECT
        S_INFO_WINDCODE,
        TRADE_DT,
        S_DQ_CLOSE,
        S_DQ_OPEN,
        S_DQ_HIGH,
        S_DQ_LOW,
        S_DQ_PCTCHANGE,
        S_DQ_AMOUNT,
        S_DQ_VOLUME,
        SUBSTRING_INDEX( S_INFO_WINDCODE, '.', 1 ) AS TICKER_SYMBOL,
        ROUND( LOG( S_DQ_PCTCHANGE / 100+1 )* 100, 4 ) AS LOG_RET,
        OPDATE as UPDATE_TIME
    FROM
        globalindexeod 
    WHERE
        1=1
        and OPDATE >= date('{update_date}')
        and OPDATE < DATE_ADD('{update_date}',INTERVAL 3 DAY)
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="global_index_eod")


def update_fund_type(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        SUBSTRING_INDEX( F_INFO_WINDCODE, ".", 1 ) AS TICKER_SYMBOL,
        left(S_INFO_SECTOR, 8) as CODE_1,
        left(S_INFO_SECTOR, 10) as CODE_2,
        left(S_INFO_SECTOR, 12) as CODE_3,
        S_INFO_SECTORENTRYDT,
        S_INFO_SECTOREXITDT,
        CUR_SIGN,
        OPDATE AS UPDATE_TIME 
    FROM
        chinamutualfundsector 
    WHERE
        1 = 1 
        AND S_INFO_SECTOR LIKE '200101%'
        and OPDATE >= "{update_date}"
    """
    fund_type = DB_CONN_JJTG_DATA.exec_query(query_sql)
    for level in range(1, 4):
        query_sql = f"""
        SELECT LEFT
            ( INDUSTRIESCODE, {8+(level-1)*2} ) AS CODE_{level},
            INDUSTRIESNAME AS INDUSTRIESNAME_{level}
        FROM
            ashareindustriescode 
        WHERE
            1 = 1 
            AND INDUSTRIESCODE LIKE '200101%' 
            AND USED = 1 
            AND LEVELNUM = "{(level-1) + 4 }"
        """
        industries_code = DB_CONN_JJTG_DATA.exec_query(query_sql)
        fund_type = fund_type.merge(industries_code, how="left")
    fund_type.drop(columns=["CODE_1", "CODE_2", "CODE_3"], inplace=True)
    DB_CONN_JJTG_DATA.upsert(fund_type, table="fund_type")


# def update_fund_tracking_idx(update_date: str = None):
#     """
#     更新被动基金跟踪指数表

#     Parameters
#     ----------
#     update_date : str, optional
#         更新日期时间,如果为空,取当前日期, by default None
#     """
#     if update_date is None:
#         update_date = LAST_TRADE_DT
#     query_sql = f"""
#         SELECT
#         SUBSTRING_INDEX( a.S_INFO_WINDCODE, '.', 1 ) AS TICKER_SYMBOL,
#         c.F_INFO_NAME AS SEC_SHORT_NAME,
#         UPPER(
#         SUBSTRING_INDEX( a.S_INFO_INDEXWINDCODE, '.', 1 )) AS IDX_SYMBOL,
#         b.S_INFO_NAME AS IDX_SHORT_NAME,
#         a.ENTRY_DT AS BEGIN_DATE,
#         a.REMOVE_DT AS END_DATE,
#         CASE
#             WHEN a.REMOVE_DT IS NULL THEN
#             1 ELSE 0
#         END AS IS_EXE,
#         a.OPDATE AS UPDATE_TIME
#     FROM
#         chinamutualfundtrackingindexzl a
#         left JOIN aindexdescription b ON a.S_INFO_INDEXWINDCODE = b.S_INFO_WINDCODE
#         left JOIN chinamutualfunddescription c ON c.F_INFO_WINDCODE = a.S_INFO_WINDCODE
#     WHERE
#         1 = 1
#         AND a.ENTRY_DT IS NOT NULL
# 	    AND a.OPDATE >= '{update_date}'
#     """
#     df = DB_CONN_WIND.exec_query(query_sql)
#     DB_CONN_JJTG_DATA.upsert(df, table="fund_tracking_idx")
#     update_sql = """
#     WITH a AS (
#         SELECT
#             a.TICKER_SYMBOL,
#             a.IDX_SYMBOL,
#             b.SecuAbbr
#         FROM
#             `fund_tracking_idx` a
#             JOIN secumainall b ON a.IDX_SYMBOL = b.SecuCode
#         WHERE
#             1 = 1
#             AND b.SecuCategoryOne = 410000
#             AND a.IDX_SHORT_NAME IS NULL
#         ) UPDATE fund_tracking_idx b
#         JOIN a ON a.TICKER_SYMBOL = b.TICKER_SYMBOL
#         SET b.IDX_SHORT_NAME = a.SecuAbbr
#     """
#     DB_CONN_JJTG_DATA.exec_non_query(update_sql)


def update_chinamutualfundtransformation(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    select * from chinamutualfundtransformation where OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="chinamutualfundtransformation")


def update_aindex_csi800_weight(update_date: str = None):
    """
    更新中证800权重

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
        select
            *,
            upper(SUBSTRING_INDEX(S_INFO_WINDCODE,'.',1)) as "TICKER_SYMBOL",
            upper(SUBSTRING_INDEX(S_CON_WINDCODE,'.',1)) as "CON_TICKER_SYMBOL"
        from
            aindexcsi800weight
        where
            OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.drop(columns=["OBJECT_ID", "OPMODE"], inplace=True)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="aindex_csi800_weight")


def update_aindex_description(update_date: str = None):
    """
    更新股票指数描述

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
        SELECT
            S_INFO_WINDCODE,
            S_INFO_NAME,
            S_INFO_COMPNAME,
            S_INFO_EXCHMARKET,
            S_INFO_INDEX_BASEPER,
            S_INFO_INDEX_BASEPT,
            S_INFO_LISTDATE,
            S_INFO_INDEX_WEIGHTSRULE,
            S_INFO_PUBLISHER,
            S_INFO_INDEXCODE,
            S_INFO_INDEXSTYLE,
            INDEX_INTRO,
            WEIGHT_TYPE,
            EXPIRE_DATE,
            INCOME_PROCESSING_METHOD,
            CHANGE_HISTORY,
            OPDATE as UPDATE_TIME,
            S_INFO_CODE as TICKER_SYMBOL
        FROM
            aindexdescription
        WHERE
            OPDATE >= "{update_date}"
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="aindex_description")


def update_aindex_eod_prices(update_date: str = None):
    """
    更新股票指数的行情

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
        SELECT
            TRADE_DT,
            S_INFO_WINDCODE,
            CRNCY_CODE,
            S_DQ_PRECLOSE,
            S_DQ_OPEN,
            S_DQ_HIGH,
            S_DQ_LOW,
            S_DQ_CLOSE,
            S_DQ_CHANGE,
            S_DQ_PCTCHANGE,
            S_DQ_VOLUME,
            S_DQ_AMOUNT,
            OPDATE,
            SUBSTRING_INDEX(S_INFO_WINDCODE,'.',1) as TICKER_SYMBOL,
            ROUND( LOG( S_DQ_PCTCHANGE / 100+1 )* 100, 4 ) AS LOG_RET
        FROM
            aindexeodprices 
        WHERE
            TRADE_DT >= "{update_date}" union
        SELECT
            TRADE_DT,
            S_INFO_WINDCODE,
            CRNCY_CODE,
            S_DQ_PRECLOSE,
            S_DQ_OPEN,
            S_DQ_HIGH,
            S_DQ_LOW,
            S_DQ_CLOSE,
            S_DQ_CHANGE,
            S_DQ_PCTCHANGE,
            S_DQ_VOLUME,
            S_DQ_AMOUNT,
            OPDATE,
            SUBSTRING_INDEX( S_INFO_WINDCODE, '.', 1 ) AS TICKER_SYMBOL,
            ROUND( LOG( S_DQ_PCTCHANGE / 100+1 )* 100, 4 ) AS LOG_RET
        FROM
            aindexwindindustrieseod
        WHERE
            TRADE_DT >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="aindex_eod_prices")


def update_aindex_members(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT

    query_sql = f"""
    SELECT
        S_INFO_WINDCODE,
        S_CON_WINDCODE,
        S_CON_INDATE,
        S_CON_OUTDATE,
        CUR_SIGN,
        OPDATE,
        upper(
        SUBSTRING_INDEX( S_INFO_WINDCODE, '.', 1 )) AS TICKER_SYMBOL,
        upper(
        SUBSTRING_INDEX( S_CON_WINDCODE, '.', 1 )) AS CON_TICKER_SYMBOL 
    FROM
        aindexmembers 
    WHERE
        1 = 1 
        AND OPDATE >= '{update_date}' UNION
    SELECT
        F_INFO_WINDCODE AS S_INFO_WINDCODE,
        S_CON_WINDCODE,
        S_CON_INDATE,
        S_CON_OUTDATE,
        CUR_SIGN,
        OPDATE,
        upper(
        SUBSTRING_INDEX( F_INFO_WINDCODE, '.', 1 )) AS TICKER_SYMBOL,
        upper(
        SUBSTRING_INDEX( S_CON_WINDCODE, '.', 1 )) AS CON_TICKER_SYMBOL 
    FROM
        aindexmemberswind 
    WHERE
        1 = 1 
        AND OPDATE >= '{update_date}' 
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="aindex_members")


def update_aindex_valuation(update_date: str = None):
    """
    更新指数估值

    Parameters
    ----------
    update_date : str, optional
       更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    select
        TRADE_DT,
        S_INFO_WINDCODE,
        CON_NUM,
        PE_LYR,
        PE_TTM,
        PB_LF,
        PCF_LYR,
        PCF_TTM,
        PS_LYR,
        PS_TTM,
        MV_TOTAL,
        MV_FLOAT,
        DIVIDEND_YIELD,
        PEG_HIS,
        TOT_SHR,
        TOT_SHR_FLOAT,
        TOT_SHR_FREE,
        TURNOVER,
        TURNOVER_FREE,
        EST_NET_PROFIT_Y1,
        EST_NET_PROFIT_Y2,
        EST_BUS_INC_Y1,
        EST_BUS_INC_Y2,
        EST_EPS_Y1,
        EST_EPS_Y2,
        EST_YOYPROFIT_Y1,
        EST_YOYPROFIT_Y2,
        EST_YOYGR_Y1,
        EST_YOYGR_Y2,
        EST_PE_Y1,
        EST_PE_Y2,
        EST_PEG_Y1,
        EST_PEG_Y2,
        SUBSTRING_INDEX(S_INFO_WINDCODE,'.',1) as TICKER_SYMBOL
     from 
        aindexvaluation
     where 
        TRADE_DT >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="aindex_valuation")


def update_cbond_curve_cnbd(update_date: str = None):
    """
    更新债券收益率

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT 
        TRADE_DT,
        B_ANAL_CURVENUMBER,
        B_ANAL_CURVENAME,
        B_ANAL_CURVETYPE,
        B_ANAL_CURVETERM,
        B_ANAL_YIELD,
        B_ANAL_BASE_YIELD,
        B_ANAL_YIELD_TOTAL,
        OPDATE
    FROM 
        cbondcurvecnbd
    WHERE OPDATE >= "{update_date}"
        AND B_ANAL_CURVETERM <=20
        AND B_ANAL_CURVENUMBER in (
            1232,1231,2142,2141,1902,1901,1252,1251,1441,1261,1851,
            1911,2171,2191,2201,2211,2221,2231,2241,2251,1442,1262,
            1852,1912,2172,2192,2202,2212,2222,2232,2242,2252,1102,
            1101,1092,1091,1072,1071,1042,1041,1502,1501,1082,1081,
            1052,1051,1062,1061,2022,2021,1481,1821,1831,1841,1871,
            1482,1822,1832,1842,1872,2462,2461,2452,2451,2472,2471,
            2482,2481,2492,2491,2502,2501,2512,2511,2522,2521,2362,
            2361,2391,2401,2411,2421,2431,2441,2272,2282,2392,2402,
            2412,2422,2432,2442,2372,2371,4042,4041,4072,4071,4052,
            4051,4062,4061,4082,4081,2182,2181,2262,2261,2032,2031,
            2042,2041,2052,2051,2062,2061)
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="cbond_curve_cnbd")


def update_ccbondvaluation(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT

    query_sql = f"""
    SELECT
        upper(
        SUBSTRING_INDEX( S_INFO_WINDCODE, '.', 1 )) AS TICKER_SYMBOL,
        S_INFO_WINDCODE,
        TRADE_DT,
        CB_ANAL_ACCRUEDDAYS,
        CB_ANAL_ACCRUEDINTEREST,
        CB_ANAL_PTM,
        CB_ANAL_CURYIELD,
        CB_ANAL_YTM,
        CB_ANAL_STRBVALUE,
        CB_ANAL_STRBPREMIUM,
        CB_ANAL_STRBPREMIUMRATIO,
        CB_ANAL_CONVPRICE,
        CB_ANAL_CONVRATIO,
        CB_ANAL_CONVVALUE,
        CB_ANAL_CONVPREMIUM,
        CB_ANAL_CONVPREMIUMRATIO,
        CB_ANAL_PARITYBASEPRICE,
        OPDATE
    FROM
        CCBondValuation c 
    WHERE
        1 = 1 
        AND OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="ccbondvaluation")


def update_fund_fee(update_date: str = None):
    """
    更新基金费率

    Parameters
    ----------
    update_date : str, optional
       更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    select 
        S_INFO_WINDCODE,
        ANN_DATE,
        FEETYPECODE,
        CHARGEWAY,
        AMOUNTDOWNLIMIT,
        AMOUNTUPLIMIT,
        HOLDINGPERIOD_DOWNLIMIT,
        HOLDINGPERIOD_UPLIMIT,
        FEERATIO,
        ISUPLIMITFEE,
        change_dt,
        SUPPLEMENTARY,
        TRADINGPLACE,
        TRADINGPLACECODE,
        HOLDINGPERIODUNIT,
        USED,
        MEMO,
        OPMODE,
        OPDATE,
        SUBSTRING_INDEX(S_INFO_WINDCODE,'.',1) as TICKER_SYMBOL, 
        OBJECT_ID as OB_ID
    from 
        CMFSubredFee
    where 
        OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="fund_fee")


def update_fund_index_eod(update_date: str = None):
    """
    更新基金指数的行情

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        TRADE_DT,
        S_INFO_WINDCODE,
        S_INFO_NAME,
        CRNCY_CODE,
        S_DQ_PRECLOSE,
        S_DQ_OPEN,
        S_DQ_HIGH,
        S_DQ_LOW,
        S_DQ_CLOSE,
        S_DQ_PCTCHANGE,
        S_DQ_CHANGE,
        S_DQ_AMOUNT,
        S_DQ_VOLUME,
        OPDATE,
        upper(SUBSTRING_INDEX(S_INFO_WINDCODE,'.',1)) as TICKER_SYMBOL,
        ROUND(LOG(S_DQ_PCTCHANGE/100+1)*100, 4)as LOG_RET
    FROM
        cmfindexeod 
    WHERE
        1=1
        and TRADE_DT >= "{update_date}"
        and S_INFO_WINDCODE is not NULL
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="fund_index_eod")


def update_fund_related_holder(update_date: str = None):
    """
    更新基金关联持有数据

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        END_DATE,
        S_INFO_WINDCODE,
        ANN_DT,
        REPORT_PERIOD,
        HOLDER_COMPCODE,
        HOLDER_NAME,
        HOLDER_TYPECODE,
        HOLD_AMOUNT,
        HOLD_RATIO,
        upper(
        SUBSTRING_INDEX( S_INFO_WINDCODE, '.', 1 )) AS TICKER_SYMBOL 
    FROM
        cmfrelatedpartiesholder 
    WHERE
        1 = 1 
        AND OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="fund_related_holder")


def update_fund_type_wind(update_date: str = None):
    """
    更新被动基金跟踪指数表

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
        SELECT 
            LEFT ( a.F_INFO_WINDCODE, 6 ) AS 'TICKER_SYMBOL',
            a.S_INFO_SECTORENTRYDT,
            a.S_INFO_SECTOREXITDT,
            b.INDUSTRIESNAME,
            a.CUR_SIGN,
            a.OPDATE
        FROM
            chinamutualfundsector a
            LEFT JOIN ashareindustriescode b ON b.INDUSTRIESCODE = a.S_INFO_SECTOR
            LEFT JOIN chinamutualfunddescription c ON c.F_INFO_WINDCODE = a.F_INFO_WINDCODE 
        WHERE
            LEFT ( a.S_INFO_SECTOR, 4 ) = '2001' 
            AND LENGTH( a.S_INFO_SECTOR ) = 16 
            AND F_INFO_SETUPDATE IS NOT NULL 
            AND a.OPDATE >= '{update_date}'
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
    DB_CONN_JJTG_DATA.upsert(df, table="fund_type_wind")


def update_md_industry_sw21(update_date: str = None):
    """
    更新股票申万行业数据

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    for level in range(2, 5):
        query_sql = f"""
        SELECT
            UPPER(
            SUBSTRING_INDEX( a.S_INFO_WINDCODE, '.', 1 )) AS TICKER_SYMBOL,
            a.S_INFO_WINDCODE,
            a.ENTRY_DT,
            a.REMOVE_DT,
            a.CUR_SIGN,
            b.LEVELNUM,
            b.INDUSTRIESNAME,
            a.OPDATE 
        FROM
            ashareswnindustriesclass a
            JOIN ashareindustriescode b ON LEFT ( a.SW_IND_CODE, {level*2} ) = LEFT ( b.INDUSTRIESCODE, {level*2} ) 
        WHERE
            1 = 1 
            AND b.USED = 1 
            AND b.LEVELNUM = {level}  	
            AND a.OPDATE >= '{update_date}'
        union SELECT
            LPAD(UPPER(
	        SUBSTRING_INDEX( a.S_INFO_WINDCODE, '.', 1 )),5,0) AS TICKER_SYMBOL,
            a.S_INFO_WINDCODE,
            a.ENTRY_DT,
            a.REMOVE_DT,
            a.CUR_SIGN,
            b.LEVELNUM,
            b.INDUSTRIESNAME,
            a.OPDATE 
        FROM
            ashareswnindustriesclass a
            JOIN ashareindustriescode b ON LEFT ( a.SW_IND_CODE, {level*2} ) = LEFT ( b.INDUSTRIESCODE, {level*2} ) 
        WHERE
            1 = 1 
            AND b.USED = 1 
            AND b.LEVELNUM = {level}  	
            AND a.OPDATE >= '{update_date}'
        """
        df = DB_CONN_WIND.exec_query(query_sql)
        df.rename(columns={"OPDATE": "UPDATE_TIME"}, inplace=True)
        DB_CONN_JJTG_DATA.upsert(df, table="md_industry_sw21")


def update_cbindex_description(update_date: str = None):
    """
    更新股票指数描述

    Parameters
    ----------
    update_date : str, optional
        更新日期时间,如果为空,取当前日期, by default None
    """
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
        SELECT
            S_INFO_WINDCODE,
            S_INFO_NAME,
            S_INFO_COMPNAME,
            S_INFO_EXCHMARKET,
            S_INFO_INDEX_BASEPER,
            S_INFO_INDEX_BASEPT,
            S_INFO_LISTDATE,
            S_INFO_INDEX_WEIGHTSRULE,
            INCOME_PROCESSING_METHOD,
            S_INFO_INDEXSTYLE,
            S_INFO_INDEX_ENAME,
            WEIGHT_TYPE,
            COMPONENT_STOCKS_NUM,
            INDEX_REGION_CODE,
            INDEX_CATEGORY_CODE,
            EXPONENTIAL_SCALE_CODE,
            WEIGHT_TYPE_CODE,
            MARKET_OWN_CODE,
            INCOME_PROCESSING_METHOD_CODE,
            EXPIRE_DATE,
            OPDATE,
            UPPER(S_INFO_CODE) as TICKER_SYMBOL
        FROM
            CBIndexDescription
        WHERE
            OPDATE >= "{update_date}" 
            union
        SELECT
            S_INFO_WINDCODE,
            S_INFO_NAME,
            S_INFO_COMPNAME,
            S_INFO_EXCHMARKET,
            S_INFO_INDEX_BASEPER,
            S_INFO_INDEX_BASEPT,
            S_INFO_LISTDATE,
            S_INFO_INDEX_WEIGHTSRULE,
            INCOME_PROCESSING_METHOD,
            S_INFO_INDEXSTYLE,
            S_INFO_INDEX_ENAME,
            WEIGHT_TYPE,
            COMPONENT_STOCKS_NUM,
            INDEX_REGION_CODE,
            INDEX_CATEGORY_CODE,
            EXPONENTIAL_SCALE_CODE,
            WEIGHT_TYPE_CODE,
            MARKET_OWN_CODE,
            INCOME_PROCESSING_METHOD_CODE,
            EXPIRE_DATE,
            OPDATE,
            UPPER( S_INFO_CODE ) AS TICKER_SYMBOL 
        FROM
            cbindexdescriptionwind
        WHERE
            OPDATE >= "{update_date}" 
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(df, table="cbindex_description")


def update_fund_performance(update_date: str = None):
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
        *,
        upper(
        SUBSTRING_INDEX( S_INFO_WINDCODE, '.', 1 )) AS TICKER_SYMBOL,
        F_AVGRETURN_QUARTER/abs(F_MAXDOWNSIDE_QUARTER)  as F_CALMAR_QUARTER,
        F_AVGRETURN_HALFYEAR/abs(F_MAXDOWNSIDE_HALFYEAR)  as F_CALMAR_HALFYEAR,
        F_AVGRETURN_YEAR/abs(F_MAXDOWNSIDE_YEAR)  as F_CALMAR_YEAR,
        F_AVGRETURN_TWOYEA/abs(F_MAXDOWNSIDE_TWOYEAR) as F_CALMAR_TWOYEAR,
        F_AVGRETURN_THREEYEAR/abs(F_MAXDOWNSIDE_THREEYEAR)  as F_CALMAR_THREEYEAR,
        F_AVGRETURN_THISYEAR/abs(F_MAXDOWNSIDE_THISYEART)  as F_CALMAR_THISYEAR,
        F_AVGRETURN_SINCEFOUND/abs(F_MAXDOWNSIDE_SINCEFOUND)  as F_CALMAR_SINCEFOUND,
        F_AVGRETURN_WEEK/abs(F_MAXDOWNSIDE_THISWEEK)  as F_CALMAR_WEEK,
        F_AVGRETURN_MONTH/abs(F_MAXDOWNSIDE_THISMONTH)  as F_CALMAR_MONTH
    FROM
        chinamfperformance 
    WHERE
        1 = 1 
        AND TRADE_DT = '{update_date}' 
    """
    df = DB_CONN_WIND.exec_query(query_sql)
    df.drop(columns=["OBJECT_ID", "OPMODE"], inplace=True)
    df.rename(
        columns={
            "OPDATE": "UPDATE_TIME",
            "F_AVGRETURN_TWOYEA": "F_AVGRETURN_TWOYEAR",
            "F_MAXDOWNSIDE_THISYEART": "F_MAXDOWNSIDE_THISYEAR",
        },
        inplace=True,
    )
    DB_CONN_JJTG_DATA.upsert(df, table="fund_performance")


def update_wind_db(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    func_list = [
        update_ashareindustriescode,
        update_chinamutualfundsector,
        update_chinamutualfundbenchmark,
        update_chinamutualfundnav,
        update_global_index_eod,
        update_fund_type,
        # update_fund_tracking_idx,
        update_chinamutualfundtransformation,
        update_aindex_csi800_weight,
        update_aindex_description,
        update_aindex_eod_prices,
        update_aindex_members,
        update_aindex_valuation,
        update_cbond_curve_cnbd,
        update_ccbondvaluation,
        update_fund_fee,
        update_fund_index_eod,
        update_fund_related_holder,
        update_fund_type_wind,
        update_md_industry_sw21,
        update_cbindex_description,
        update_fund_performance,
    ]

    for i, func in enumerate(func_list, start=1):
        try:
            for _ in range(5):
                func(update_date=update_date)
                print(f"{func.__name__}完成写入")
                print("==" * 35)
                break
        except Exception as e:
            print(f"失败{i}:{func.__name__},原因是:{e}")


if __name__ == "__main__":
    update_chinamutualfundnav()
