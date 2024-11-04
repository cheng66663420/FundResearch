# -*- coding: utf-8 -*-

import logging
import time

from tqdm import tqdm

import quant_utils.data_moudle as dm
from quant_utils.constant import LOG_FILE_PATH, LOG_FORMAT
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_JY, DB_CONN_JY_LOCAL
from quant_utils.utils import yield_split_list

DB_CONN = DB_CONN_JJTG_DATA
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format=LOG_FORMAT,
)


def get_local_max_jsid(table: str, jsid_name: str = "JSID") -> int:
    """
    获取本地表中最大jsid

    Parameters
    ----------
    table : str
        表名
    jsid_name : str, optional
        本地库中jsid的名字, by default "JYJSID"

    Returns
    -------
    int
        最大的jsid
    """
    query_sql = f"""
        select ifnull(max({jsid_name}),0) as jsid from {table}
    """
    return DB_CONN.exec_query(query_sql)["jsid"].values[0]


def query_from_remote_upsert_into_local(
    query_sql: str,
    table: str,
    query_db_conn,
    upsert_db_conn=DB_CONN_JJTG_DATA,
    if_rename: int = 1,
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

    if if_rename:
        df.rename(
            columns={
                "TRADE_DATE": "TRADE_DT",
                "OPDATE": "UPDATE_TIME",
                "update_time": "UPDATE_TIME",
                "EndDate": "TRADE_DT",
                "InsertTime": "CREATE_TIME",
                "UpdateTime": "UPDATE_TIME",
                "XGRQ": "UPDATE_TIME",
            },
            inplace=True,
        )
    # print(df)
    # 写入数据
    # df = df.where(df.notnull(), None)
    df = df.drop_duplicates()
    if not df.empty:
        jsid_list = df["JSID"].tolist()
        jsid_list.sort()
        for i in tqdm(yield_split_list(jsid_list, 500000)):
            df_temp = df[df["JSID"].isin(i)]
            upsert_db_conn.upsert(df_to_upsert=df_temp, table=table)
    else:
        print(f"{table}-无数据插入!")


def update_fund_asset_own_jy():
    """
    利用聚源数据更新fund_asset_own_jy
    """
    table = "fund_asset_own"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        c.SecuCode AS TICKER_SYMBOL,
        a.EndDate AS REPORT_DATE,
        a.InfoPublDate AS PUBLISH_DATE,
        ifnull(a.TotalAsset,0) AS TOTAL_ASSET,
        ifnull(a.NV,0) AS NET_ASSET,
        ifnull(e.NetAssetsValue,0) AS NET_ASSET_VALUE,
        ifnull(d.EndShares,0) AS END_SHARE,
        ifnull(d.SharesChange,0) AS SHARES_CHANGE,
        ifnull(a.MVOfMonetary,0) AS CASH_MARKET_VALUE,
        ifnull(a.MVOfMonetary,0) / a.NV * 100 AS CASH_RATIO_IN_NA,
        ifnull(a.MVOfMonetary / a.TotalAsset * 100,0) AS CASH_RATIO_IN_TA,
        ifnull(a.MVOfBond,0) AS BOND_MARKET_VALUE,
        ifnull(a.MVOfBond / a.NV * 100, 0) AS BOND_RATIO_IN_NA,
        ifnull(a.MVOfBond / a.TotalAsset * 100,0) AS BOND_RATIO_IN_TA,
        ifnull(a.MVOfAssetBacked,0) AS ABS_MARKET_VALUE,
        ifnull(a.MVOfAssetBacked / a.NV * 100,0) AS ABS_RATIO_IN_NA,
        ifnull(a.MVOfAssetBacked / a.TotalAsset * 100,0) AS ABS_RATIO_IN_TA,
        ifnull(a.MVOfEquity, 0) AS EQUITY_MARKET_VALUE,
        ifnull(a.MVOfEquity / a.NV * 100,0) AS EQUITY_RATIO_IN_NA,
        ifnull(a.MVOfEquity / a.TotalAsset * 100,0) AS EQUITY_RATIO_IN_TA,
        ifnull(a.MVOfFund,0) AS FUND_MARKET_VALUE,
        ifnull(a.MVOfFund / a.NV * 100, 0) AS FUND_RATIO_IN_NA,
        ifnull(a.MVOfFund / a.TotalAsset * 100, 0) AS FUND_RATIO_IN_TA,
        ifnull(a.MVOfOtherI, 0) AS OTHER_MARKET_VALUE,
        ifnull(a.MVOfOtherI / a.NV * 100,0) AS OTHER_RATIO_IN_NA,
        ifnull(a.MVOfOtherI / a.TotalAsset * 100,0) AS OTHER_RATIO_IN_TA,
        ifnull(a.MVOfTreasuries,0) AS F_PRT_GOVBOND,
        ifnull(a.MVOfTreasuries / a.NV * 100,0) AS F_PRT_GOVBONDTONAV,
        ifnull(a.RIAOfCentralBank*100,0) AS F_PRT_CTRBANKBILL,
        ifnull(a.RIAOfCentralBank / a.NV * 100,0) AS F_PRT_CTRBANKBILLTONAV,
        ifnull(a.MVOfFinancial, 0) AS F_PRT_FINANBOND,
        ifnull(a.MVOfFinancial / a.NV * 100,0) AS F_PRT_FINANBONDTONAV,
        ifnull(a.MVOfPolicyBond,0) AS F_PRT_POLIFINANBDVALUE,
        ifnull(a.MVOfPolicyBond / a.NV * 100, 0) AS F_PRT_POLIFINANBDTONAV,
        ifnull(a.MVOfMediumTerm,0) AS F_PRT_MTNVALUE,
        ifnull(a.MVOfMediumTerm / a.NV * 100,0) F_PRT_MTNONAV,
        ifnull(a.MVOfConvertible, 0) AS F_PRT_COVERTBOND,
        ifnull(a.MVOfConvertible / a.NV * 100,0) F_PRT_COVERTBONDTONAV,
        ifnull(a.MVOfOtherBonds,0) AS OTH_BDVALUE,
        ifnull(a.MVOfOtherBonds / a.NV * 100,0) AS OTH_BDTONAV,
        ifnull(a.MVOfShortTerm, 0) AS F_PRT_CPVALUE,
        ifnull(a.MVOfShortTerm / a.NV * 100, 0) AS F_PRT_CPTONAV,
        ifnull(a.RIAOfNCDs*100, 0) AS F_PRT_CDS,
        ifnull(a.RIAOfNCDs / a.nv * 100,0) AS F_PRT_CDSTONAV,
        ifnull(a.MVOfLocalGov, 0) AS F_PRT_LOCALGOVBOND,
        ifnull(a.MVOfLocalGov / a.nv * 100, 0) F_PRT_LOCALGOVBONDTONAV,
        ifnull(a.MVOfMinorEnterp, 0) AS F_PRT_SMALLCORPBOND,
        ifnull(a.MVOfMinorEnterp / a.nv * 100, 0) F_PRT_SMALLCORPBONDTONAV,
        ifnull(a.MVOfFloRateBond,0) AS F_PRT_FLOATINTBONDOVER397,
        ifnull(a.MVOfFloRateBond / a.nv * 100, 0) AS F_PRT_FLOATINTBONDOVER397TONAV,
        ifnull(a.MVOfHKConnect, 0) AS F_PRT_HKSTOCKVALUE,
        ifnull(a.MVOfHKConnect / a.nv * 100, 0) F_PRT_HKSTOCKVALUETONAV,
        ifnull(a.RIAOfMetals * 100,0) as RIAOfMetals,
        ifnull(a.RINOfMetals * 100,0) as RINOfMetals,
        ifnull(MVOfCorporate, 0) as F_PRT_CORPBOND,
        ifnull(RINOfCorporate*100, 0) as F_PRT_CORPBONDTONAV,
        a.UpdateTime AS UPDATE_TIME,
        d.JSID
    FROM
        mf_assetallocationall a
        left JOIN mf_fundarchives b ON a.InnerCode = b.InnerCode
        left JOIN mf_fundarchives c ON c.maincode = b.SecuCode
        left JOIN mf_shareschange d ON d.InnerCode = c.InnerCode 
        AND d.EndDate = a.EndDate
        left JOIN mf_mainfinancialindexq e ON e.InnerCode = d.InnerCode 
        AND e.EndDate = d.EndDate 
    WHERE
        1 = 1
        AND a.ReportType NOT IN (5,6)
        AND d.StatPeriod = 3
        and d.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_info_jy():
    table = "fund_info"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    WITH a AS (
        SELECT
            a.InnerCode AS SECURITY_ID,
            b.InnerCode AS FUND_ID,
            a.SecuCode AS TICKER_SYMBOL,
            a.MainCode AS TICKER_SYMBOL_M,
            c.ChiNameAbbr AS SEC_SHORT_NAME,
            c.ChiName as SEC_FULL_NAME,
            a.EstablishmentDate AS ESTABLISH_DATE,
            a.ExpireDate AS EXPIRE_DATE,
            a.JSID,
        CASE
                a.MainCode 
                WHEN a.SecuCode THEN
                1 ELSE 0 
            END AS IS_MAIN,
        CASE
                
                WHEN c.ChiName LIKE '%指数增强%' THEN
                'EI' 
                WHEN c.ChiName LIKE '%指数%' THEN
                "I" 
            END AS INDEX_FUND,
        CASE
                a.IfFOF 
                WHEN 1 THEN
                1 ELSE 0 
            END AS IS_FOF,
        CASE
                
                WHEN c.ChiName REGEXP 'QDII' or c.ChiNameAbbr REGEXP 'QDII' THEN
                1 ELSE 0 
            END AS IS_QDII,
        CASE
                c.ChiName REGEXP '定开|持有|封闭|定期|滚动' 
                WHEN 1 THEN
                1 ELSE 0 
            END AS `IS_ILLIQUID`,
        CASE
                c.ChiName REGEXP '联接' or c.ChiNameAbbr REGEXP '联接'
                WHEN 1 THEN
                1 ELSE 0 
            END AS IS_ETF_LINK,
            d.InvestAdvisorCode AS MANAGEMENT_COMPANY,
            d.InvestAdvisorAbbrName AS MANAGEMENT_COMPANY_NAME,
            e.TrusteeCode AS CUSTODIAN,
            e.TrusteeName AS CUSTODIAN_NAME,
            a.FundTypeCode AS CATEGORY,
            a.FundType AS CATEGORY_NAME,
            a.InvestTarget AS INVEST_TARGET,
            a.InvestField AS INVEST_FIELD,
            a.PerformanceBenchMark AS PERF_BENCHMARK,
            a.ProfitDistributionRule AS DIVI_DESC

        FROM
            mf_fundarchives a
            LEFT JOIN mf_fundarchives b ON a.MainCode = b.SecuCode
            LEFT JOIN secumain c ON a.InnerCode = c.InnerCode
            LEFT JOIN mf_investadvisoroutline d ON d.InvestAdvisorCode = a.InvestAdvisorCode
            LEFT JOIN mf_trusteeoutline e ON e.TrusteeCode = a.TrusteeCode 
        WHERE
            1 = 1 
            AND a.JSID >= '{max_jsid}' 
        ) SELECT
            *,
            CASE
                WHEN SEC_SHORT_NAME LIKE '%LOF%' THEN
                'LOF' 
                WHEN ( SEC_SHORT_NAME REGEXP 'ETF' AND IS_ETF_LINK = 0 ) THEN
                "ETF" 
            END AS ETF_LOF 
        FROM
            a
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_holdings_q_jy():
    table = "fund_holdings_q"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ReportDate AS REPORT_DATE,
        a.InfoPublDate AS PUBLISH_DATE,
        a.InnerCode AS FUND_ID,
        "E" AS SECURITY_TYPE,
        b.SecuCode AS HOLDING_TICKER_SYMBOL,
        b.SecuAbbr AS HOLDING_SEC_SHORT_NAME,
        a.MarketValue AS MARKET_VALUE,
        a.SharesHolding AS HOLD_VOLUME,
        a.RatioInNV AS RATIO_IN_NA,
        a.MarketValue/c.MVOfEquity as RATIO_IN_EQUITY,
        a.SerialNumber AS HOLDING_NUM,
        b.CurrencyCode AS CURRENCY_CD,
        b.SecuMarket AS EXCHANGE_CD,
        a.JSID,
        a.InsertTime,
        a.XGRQ 
    FROM
        mf_keystockportfolio a
        JOIN secumainall b ON a.StockInnerCode = b.InnerCode 
        JOIN mf_assetallocationall c ON c.InnerCode = a.InnerCode 
	    AND c.EndDate = a.ReportDate 
    WHERE
        1 = 1
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_holdings_jy():
    table = "fund_holdings"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ReportDate AS REPORT_DATE,
        a.InfoPublDate AS PUBLISH_DATE,
        a.InnerCode AS FUND_ID,
        "E" AS SECURITY_TYPE,
        b.SecuCode AS HOLDING_TICKER_SYMBOL,
        b.SecuAbbr AS HOLDING_SEC_SHORT_NAME,
        a.MarketValue AS MARKET_VALUE,
        a.SharesHolding AS HOLD_VOLUME,
        a.RatioInNV AS RATIO_IN_NA,
        a.MarketValue/c.MVOfEquity as RATIO_IN_EQUITY,
        a.SerialNumber AS HOLDING_NUM,
        b.CurrencyCode AS CURRENCY_CD,
        b.SecuMarket AS EXCHANGE_CD,
        a.JSID,
        a.InsertTime,
        a.XGRQ 
    FROM
        mf_stockportfoliodetail a
        JOIN secumainall b ON a.StockInnerCode = b.InnerCode
        JOIN mf_assetallocationall c ON c.InnerCode = a.InnerCode 
	    AND c.EndDate = a.ReportDate 
    WHERE
        1 = 1 
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_holder_info():
    table = "fund_holder_info"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        InnerCode AS FUND_ID,
        EndDate AS REPORT_DATE,
        InfoPublDate AS PUBLISH_DATE,
        HolderAccountNumber AS HOLDER_NUMBER,
        AverageHoldShares AS AVG_HOLD_SHARES,
        InstitutionHoldShares AS INST_HOLD_SHARES,
        InstitutionHoldRatio AS INST_HOLD_RATIO,
        IndividualHoldshares AS INDI_HOLD_SHARES,
        IndividualHoldRatio AS INDI_HOLD_RATIO,
        UndefinedHoldShares AS UNDEFINED_HOLD_SHARES,
        UndefinedHoldRatio AS UNDEFINED_HOLD_RATIO,
        Top10HolderAmount AS TOP10_HOLD_SHARES,
        Top10HoldersProportion AS TOP10_HOLD_RATIO,
        ProfessionalHoldShares AS PRAC_HOLD_SHARES,
        ProfessionalHoldRatio AS PRAC_HOLD_RATIO,
        ETFFeederHoldRatio AS ETF_FREEDER_RATIO,
        ETFFeederHoldShares AS ETF_FREEDER_SHARES,
        SeniorManagementStart AS SENIOR_MANAGERMENT_START,
        SeniorManagementEnd AS SENIOR_MANAGERMENT_END,
        FundManagerStart AS FUND_MANAGER_START,
        FundManagerEnd AS FUND_MANAGER_END,
        InfoSource AS INFO_SOURCE,
        XGRQ AS UPDATE_TIME,
        InsertTime AS CREATE_TIME,
        JSID 
    FROM
        mf_holderinfo
    WHERE
        1 = 1
        and JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_manager_info_jy():
    table = "fund_manager_info"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        B.SECUCODE AS TICKER_SYMBOL,
        B.SECUABBR AS SEC_SHORT_NAME,
    CASE
            A.POSTNAME 
            WHEN 1 THEN
            "FM" 
            WHEN 2 THEN
            "FMA" 
        END AS POSITION,
        PERSONALCODE AS PERSON_ID,
        A.`NAME` AS NAME,
        A.ACCESSIONDATE AS ACCESSION_DATE,
        A.DIMISSIONDATE AS DIMISSION_DATE,
        A.INFOPUBLDATE AS PUBLISH_DATE,
        A.GENDER AS GENDER,
        A.BIRTHYMINFO AS BIRTHYM_INFO,
        A.AGE AS AGE,
        A.EXPERIENCEYEAR AS EXPERIENCE_YEAR,
        A.PRACTICEDATE AS PRACTICE_DATE,
        A.BACKGROUND,
        A.INCUMBENT,
        A.INFOSOURCE AS INFO_SOURCE,
        A.REMARK,
        A.UPDATETIME AS UPDATE_TIME,
        A.INSERTTIME AS CREATE_TIME,
        A.JSID 
    FROM
        MF_PERSONALINFOCHANGE A
        JOIN SECUMAIN B ON A.INNERCODE = B.INNERCODE 
    WHERE
        1 =1
        and A.JSID >= "{max_jsid}"
    """

    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_share_change_jy():
    table = "fund_share_change"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        A.ENDDATE AS REPORT_DATE,
        C.SECUCODE AS TICKER_SYMBOL,
        C.CHINAME AS SEC_SHORT_NAME,
        A.STATPERIOD AS REPORT_TYPE,
        MS AS REPORT_TYPE_NAME,
        A.ENDDATE AS END_DATE,
        A.STARTSHARES AS START_SHARE,
        A.APPLYINGSHARES AS APPLY_SHARE,
        A.REDEEMSHARES AS REDEEM_SHARE,
        A.SPLITSHARES AS SPLIT_SHARE,
        A.ENDSHARES AS END_SHARE,
        A.SHARESCHANGE AS SHARES_CHANGE,
        A.RATEOFSHARESCHANGE AS SHARES_CHANGE_RATIO,
        A.FLOATSHARES AS FLOAT_SHARES,
        A.DIVIDENDREINVESTMENT AS DIVIDEND_REINVESTMENT,
        A.SHGIFTIN AS SHGIFT_IN,
        A.SHIFTOUT AS SHIFT_OUT,
        A.IFCOMBINE AS IF_COMBINE,
        A.INFOPUBLDATE AS PUBLISH_DATE,
        A.XGRQ AS UPDATE_TIME,
        A.InsertTime AS CREATE_TIME,
        A.JSID 
    FROM
        MF_SHARESCHANGE A
        JOIN SECUMAIN C ON A.INNERCODE = C.INNERCODE
        JOIN CT_SYSTEMCONST ON A.STATPERIOD = DM 
    WHERE
        1 = 1 
        AND LB = 1087 
        AND DM IN (
            3,
            6,
            12,
            993,
        995,
        996)
        and A.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_adj_nav():
    table = "fund_adj_nav"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.TradingDay AS END_DATE,
        b.SecuCode AS TICKER_SYMBOL,
        a.UnitNV as UNIT_NAV,
        a.UnitNVRestored AS ADJ_NAV,
        a.GrowthRateFactor AS ADJ_FACTOR,
        a.NVRDailyGrowthRate AS RETURN_RATE,
        log( 1 + a.NVRDailyGrowthRate / 100 )* 100 AS LOG_RET,
        a.UpdateTime AS UPDATE_TIME,
        a.JSID
    FROM
        mf_netvalueretranstwo a
        JOIN secumain b ON a.InnerCode = b.InnerCode 
    WHERE
        1=1
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )
    query_sql = """
	SELECT
		a.TICKER_SYMBOL,
		max( a.END_DATE ) AS NAV_END_DATE 
	FROM
		fund_adj_nav a
		JOIN fund_info b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
	WHERE
		b.EXPIRE_DATE IS NULL 
	GROUP BY
	TICKER_SYMBOL 
    """
    temp = DB_CONN_JJTG_DATA.exec_query(query_sql)
    DB_CONN_JJTG_DATA.upsert(temp, table="fund_perf_desc")


def update_fund_nav_gr():
    table = "fund_nav_gr"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        b.SecuCode AS TICKER_SYMBOL,
        a.TradingDay AS END_DATE,
        b.InnerCode AS SECURITY_ID,
        a.NVDailyGrowthRate AS RETURN_RATE,
        a.RRInSelectedWeek AS RETURN_RATE_WTD,
        a.RRInSingleWeek AS RETURN_RATE_1W,
        a.RRInSelectedMonth AS RETURN_RATE_MTD,
        a.RRInSingleMonth AS RETURN_RATE_1M,
        a.RRInThreeMonth AS RETURN_RATE_3M,
        a.RRInSixMonth AS RETURN_RATE_6M,
        a.RRSinceThisYear AS RETURN_RATE_YTD,
        RRInSingleYear AS RETURN_RATE_1Y,
        RRInTwoYear AS RETURN_RATE_2Y,
        RRInThreeYear AS RETURN_RATE_3Y,
        RRInFiveYear AS RETURN_RATE_5Y,
        RRSinceStart AS RETURN_RATE_EST,
        CASE c.IfYearEnd 
        WHEN 1 THEN
            1
        ELSE
            0
    END as IS_YEAR,
    a.UpdateTime as UPDATE_TIME,
    a.JSID

    FROM
        MF_NetValuePerformanceHis a
        JOIN secumain b ON b.InnerCode = a.InnerCode
        JOIN QT_TradingDayNew c ON c.TradingDate = a.TradingDay 
    WHERE
        1 = 1 
        AND c.SecuMarket = 83 
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_calmar_ratio():
    table = "fund_calmar_ratio"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        b.SecuCode AS TICKER_SYMBOL,
        a.EndDate AS TRADE_DT,
        a.IndexCode,
        a.IndexName,
        a.IndexCycle,
        a.DataValue,
        a.InsertTime,
        a.UpdateTime,
        a.JSID 
    FROM
        mf_calmarratio a
        JOIN secumainall b ON a.InnerCode = b.InnerCode 
    WHERE
        1 = 1 
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_fee_new():
    table = "fund_fee_new"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        b.InnerCode AS SECURITY_ID,
        b.SecuCode AS TICKER_SYMBOL,
        InfoPublDate AS PUBLISH_DATE,
        CASE
            IfExecuted 
            WHEN 1 THEN
            1 ELSE 0 
        END AS IS_EXE,
        ExcuteDate AS BEGIN_DATE,
        CancelDate AS END_DATE,
        ChargeRateType AS CHARGE_TYPE,
        ChargeRateTyDes AS CHARGE_TYPE_CN,
        ChargeRateDiv AS CHARGE_DIV,-- 	f.MS AS CHARGE_DIV_CN,
        ChargeRateCur AS CHARGE_EXCH_CD,
        d.MS AS CHARGE_EXCH_CN,
        CASE
            WHEN ChargeRateTyDes LIKE '%前端%' THEN
            1 
            WHEN ChargeRateTyDes LIKE '%后端%' THEN
            2 ELSE NULL 
        END AS CHARGE_MODE,
        CASE
            WHEN ChargeRateTyDes LIKE '%前端%' THEN
            '前端' 
            WHEN ChargeRateTyDes LIKE '%后端%' THEN
            '后端' ELSE NULL 
        END AS CHARGE_MODE_CN,
        a.ClientType AS CLIENT_TYPE,
        c.MS AS CLIENT_TYPE_CN,
        a.ShiftInTarget AS SHIFTT_IN_TARGET,
        MinChargeRate AS MIN_CHAR_RATE,
        MaxChargeRate AS MAX_CHAR_RATE,
        ChargeRateUnit AS CHARGE_UNIT,
        ChargeRateDes AS CHARGE_DESC,
        DivStandUnit1 AS CHAR_CON_UNIT1,
        e.MS AS CHAR_CON_UNIT1_CN,
        StDivStand1 AS CHAR_START1,
        EnDivStand1 AS CHAR_END1,
        IfApplyStart1 AS IS_CHAR_START1,
        IfApplyEnd1 AS IS_CHAR_END1,
        StDivStand2 AS CHAR_START2,
        EnDivStand2 AS CHAR_END2,
        IfApplyStart2 AS IS_CHAR_START2,
        IfApplyEnd2 AS IS_CHAR_END2,
        StDivStand3 AS CHAR_START3,
        EnDivStand3 AS CHAR_END3,
        IfApplyStart3 AS IS_CHAR_START3,
        IfApplyEnd3 AS IS_CHAR_END3,
        InsertTime AS UPDATE_TIME,
        a.JSID 
    FROM
        mf_chargeratenew a
        JOIN secumain b ON a.InnerCode = b.InnerCode
        LEFT JOIN CT_SystemConst c ON a.ClientType = c.DM 
        AND c.LB = 1807
        LEFT JOIN CT_SystemConst d ON a.ChargeRateCur = d.DM 
        AND d.LB = 1068 
        AND d.DM IN ( 1000, 1100, 1160, 1420 )
        LEFT JOIN CT_SystemConst e ON a.DivStandUnit1 = e.DM 
        AND e.LB = 1208
        LEFT JOIN CT_SystemConst f ON a.ChargeRateDiv = f.DM 
        AND f.LB = 1898 
    WHERE
        1 = 1
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_holding_bond():
    table = "fund_holding_bond"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ReportDate AS REPORT_DATE,
        b.InnerCode AS SECURITY_ID,
        b.SecuCode AS TICKER_SYMBOL,
        c.InnerCode AS HOLIDING_SECURITY_ID,
        c.SecuCode AS HOLDDING_TICKER_SYMBOL,
        c.SecuAbbr AS HOLDING_SEC_SHORT_NAME,
        c.SecuCategory,
        a.IfInConvertibleTerm,
        a.MarketValue,
        a.RatioInNV,
        a.SerialNumber,
        e.FirstIndustryName,
        e.SecondIndustryName,
        e.ThirdIndustryName,
        a.XGRQ as UPDATE_TIME,
        a.JSID 
    FROM
        mf_bondportifoliodetail a
        JOIN secumain b ON a.InnerCode = b.InnerCode
        JOIN secumain c ON c.InnerCode = a.BondCode
        LEFT JOIN Bond_BIIndustry e ON e.CompanyCode = c.CompanyCode 
    WHERE
        1 = 1 
        and a.JSID >= "{max_jsid}"
        AND e.Standard = 38 
    ORDER BY
        ReportDate,
        TICKER_SYMBOL,
        SerialNumber
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_fund_equ_trade():
    table = "fund_equ_trade"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        ReportDate AS REPORT_DATE,
        InnerCode AS FUND_ID,
        InfoSource as REPORT_TYPE,
        BuyingCost as BUY_COST,
        SellingIncome as SELL_INCOME,
        InfoPublDate as PUBLISH_DATE,
        TurnoverRate,
        TurnoverRateOneY,
        InsertTime,
        XGRQ,
        JSID 
    FROM
        mf_fundtradeinfo
    WHERE
        1 = 1 
        and JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
    )


def update_jy_tbales():
    tables = [
        "bond_code",
        "bond_conbdissueproject",
        "bond_creditgrading",
        "secumain",
        "secumainall",
        "qt_goldtrademarket",
    ]
    for table in tables:
        max_jsid = get_local_max_jsid(table=table)
        query_sql = f"select * from {table} where JSID >= {max_jsid}"
        query_from_remote_upsert_into_local(
            query_sql=query_sql,
            table=table,
            query_db_conn=DB_CONN_JY,
            upsert_db_conn=DB_CONN,
            if_rename=0,
        )


def update_fund_income_statement():
    table = "fund_income_statement"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        b.SecuCode AS TICKER_SYMBOL,
        a.*
    FROM
        mf_incomestatementnew a
        JOIN secumain b ON a.InnerCode = b.InnerCode 
    WHERE
        1 = 1 
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
        if_rename=0,
    )


def update_fund_balance_sheet():
    table = "fund_balance_sheet"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        b.SecuCode AS TICKER_SYMBOL,
        a.*
    FROM
        mf_balancesheetnew a
        JOIN secumain b ON a.InnerCode = b.InnerCode 
    WHERE
        1 = 1 
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
        if_rename=0,
    )


def update_jy_indexquote():
    table = "jy_indexquote"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
        SELECT
            a.ID,
            b.SecuCode,
            a.TradingDay,
            a.PrevClosePrice,
            a.OpenPrice,
            a.HighPrice,
            a.LowPrice,
            a.ClosePrice,
            a.TurnoverVolume,
            a.TurnoverValue,
            a.TurnoverDeals,
            a.ChangePCT,
            a.NegotiableMV,
            a.XGRQ,
            a.JSID 
        FROM
            qt_indexquote a
            JOIN secumain b ON a.InnerCode = b.InnerCode 
        WHERE
            1 = 1 
            and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
        if_rename=0,
    )


def update_aindex_sw_eod():
    """
    更新申万指数行情

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    table = "aindex_sw_eod"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.TradingDay AS TRADE_DT,
        b.SecuCode AS TICKER_SYMBOL,
        b.SecuAbbr AS SEC_SHORT_NAME,
        PrevClosePrice AS S_DQ_PRECLOSE,
        OpenPrice AS S_DQ_OPEN,
        HighPrice AS S_DQ_HIGH,
        LowPrice AS S_DQ_LOW,
        ClosePrice AS S_DQ_CLOSE,
        TurnoverVolume AS S_DQ_VOLUME,
        TurnoverValue AS S_DQ_AMOUNT,
        ChangePCT AS S_DQ_PCT_CHANGE,
        IndexPE as INDEX_PE,
        IndexPB as INDEX_PB,
        TotalMarketValue as TOTAL_MARKET_VALUE,
        AShareTotalMV as ASHARE_TOTAL_MV,
        UpdateTime AS UPDATE_TIME,
        a.JSID
    FROM
        qt_sywgindexquote a
        JOIN SecuMain b ON b.InnerCode = a.InnerCode 
    WHERE
        1=1
        and a.JSID >= "{max_jsid}"
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY_LOCAL
    )


def update_bond_chinabondindexquote():
    """
    更新债券指数收益率数据

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    table = "bond_chinabondindexquote"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.TradingDay AS TRADE_DT,
        b.SecuCode AS TICKER_SYMBOL,
        b.SecuAbbr AS SEC_SHORT_NAME,
        a.IndexType,
        a.PrevClosePrice AS S_DQ_PRECLOSE,
        a.ClosePrice AS S_DQ_CLOSE,
        a.ChangePCT AS S_DQ_PCTCHANGE,
        a.Duration1,
        a.Duration2,
        a.Convexity1,
        a.Convexity2,
        a.YTM,
        a.TTM,
        a.InterestPaymentRate,
        a.TotalMarketValue,
        a.DirtyPriceIndex,
        a.NetPriceIndex,
        a.YTMByMarketValue,
        a.AvgBasicPointValue,
        a.PreDirtyPriceIndex,
        a.PreNetPriceIndex,
        a.DirtyPriceIndexChangePCT,
        a.NetPriceIndexChangePCT,
        a.SpotSettVol,
        a.MainCode,
        a.IndexPrevClosePrice,
        a.IndexClosePrice,
        a.IndexChangePCT,
        a.UpdateTime AS UPDATE_TIME,
        a.JSID
    FROM
        bond_chinabondindexquote a
        JOIN secumain b ON b.InnerCode = a.InnerCode
    WHERE
        a.JSID >= '{max_jsid}'
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table="bond_chinabondindexquote",
        query_db_conn=DB_CONN_JY,
    )


def update_qt_interestrateindex():
    table = "qt_interestrateindex"
    max_jsid = get_local_max_jsid(table=table)

    query_sql = f"""
    select * 
    from 	
        qt_interestrateindex 
    WHERE
        1 = 1
        and jsid >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
        upsert_db_conn=DB_CONN,
        if_rename=1,
    )


def update_fund_stock_portfolio_change():
    """
    更新基金股票组合变动数据

    Parameters
    ----------
    update_date : str, optional
        更新日期, by default None
    """
    table = "fund_stock_portfolio_change"
    max_jsid = get_local_max_jsid(table=table)

    query_sql = f"""
    SELECT
        s.secucode AS TICKER_SYMBOL,
        a.ReportDate AS REPORT_DATE,
        a.InfoPublDate AS PUBLISH_DATE,
        a.AccumulatedTradeSum,
        a.RatioInNVAtBegin,
        a.RatioInNVAtEnd,
        c.MS,
        b.SecuCode AS HOLDING_TICKER_SYMBOL,
        b.ChiNameAbbr AS SEC_SHORT_NAME,
        a.InsertTime AS UPDATE_TIME,
        a.JSID 
    FROM
        `mf_stockportfoliochange` a
        JOIN secumain b ON a.StockInnerCode = b.InnerCode
        JOIN CT_SystemConst c ON a.ChangeType = c.DM
        JOIN secumain s ON a.InnerCode = s.InnerCode 
    WHERE
        1 = 1 
        AND c.LB = 1095 
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY,
    )


def del_data(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
        SELECT
            a.TABLE_NAME,
            b.DEL_JSID
        FROM
            `chentiancheng`.`md_tables` a
            JOIN jy_local.`del_table` b ON a.TABLE_NAME = b.TABLE_NAME 
        WHERE
            1 = 1
            and a.IF_USED = 1
            and DATA_SOURCE = 'jy'
            and b.UPDATE_TIME >= '{update_date}'
    """
    del_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    for table in del_df["TABLE_NAME"].unique():
        del_temp = del_df.query(f"TABLE_NAME == '{table}'")
        id_list = del_temp["DEL_JSID"].tolist()
        id_list = [str(i) for i in id_list]
        id_str = ",".join(id_list)
        jsid_sql = f"""
            select ID as DEL_ID, JSID as DEL_JSID from {table.lower()} where JSID in ({id_str})
        """
        del_data = DB_CONN_JY_LOCAL.exec_query(jsid_sql)
        if not del_data.empty:
            del_sql = f"""
                delete from {table.lower()}  where JSID in ({id_str})
            """
            DB_CONN_JY_LOCAL.exec_non_query(del_sql)
            time.sleep(1)
        else:
            print("无需删除")


def update_fund_holdings_fund():
    table = "fund_holdings_fund"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.*,
        b.SecuCode AS HOLDING_TICKER_SYMBOL 
    FROM
        jy_local.mf_fundportifoliodetail a
        JOIN jy_local.secumainall b ON b.InnerCode = a.FundInnerCode
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql,
        table=table,
        query_db_conn=DB_CONN_JY_LOCAL,
    )


def update_ed_consumerpriceindex():
    table = "ed_consumerpriceindex"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        *
    FROM
        ed_consumerpriceindex
    where
        1=1
        and JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_fund_purchaseandredeemn():
    table = "fund_purchaseandredeemn"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ID,
        a.EndDate AS TRADE_DT,
        b.SecuCode AS TICKER_SYMBOL,
        ApplyingTypeI,
        RedeemTypeI,
        ApplyingMaxI,
        ApplyingTypeII,
        RedeemTypeII,
        ApplyingMaxII,
        ApplyingTypeIII,
        RedeemTypeIII,
        ApplyingMaxIII,
        ApplyingTypeIV,
        RedeemTypeIV,
        ApplyingMaxIV,
        ApplyingTypeV,
        RedeemTypeV,
        ApplyingMaxV,
        ApplyingTypeVI,
        RedeemTypeVI,
        ApplyingMaxVI,
        ApplyingTypeVII,
        RedeemTypeVII,
        ApplyingMaxVII,
        ApplyingTypeVIII,
        RedeemTypeVIII,
        ApplyingMaxVIII,
        a.InsertTime,
        a.UpdateTime,
        a.JSID 
    FROM
        mf_purchaseandredeemn a
        JOIN secumain b ON a.InnerCode = b.InnerCode 
    WHERE
        1 =1
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_fund_type_jy():
    table = "fund_type_jy"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ID,
        a.InnerCode AS SECURITY_ID,
        b.SecuCode AS TICKER_SYMBOL,
        a.EffectiveDate AS S_INFO_SECTORENTRYDT,
        a.CancelDate AS S_INFO_SECTOREXITDT,
        a.IfEffected AS CUR_SIGN,
        a.FirstAssetCatName AS INDUSTRIESNAME_1,
        a.SecAssetCatName AS INDUSTRIESNAME_2,
        a.ThirdAssetCatName AS INDUSTRIESNAME_3,
        a.TransCode,
        a.InsertTime AS CREATE_TIME,
        a.UpdateTime AS UPDATE_TIME,
        a.JSID 
    FROM
        mf_jyfundtype a
        JOIN secumainall b ON a.InnerCode = b.InnerCode
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_fund_tracking_idx():
    table = "fund_tracking_idx"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ID,
        b.SecuCode AS TICKER_SYMBOL,
        b.SecuAbbr AS SEC_SHORT_NAME,
        c.SecuCode AS IDX_SYMBOL,
        c.SecuAbbr AS IDX_SHORT_NAME,
        a.IfExecuted AS IS_EXE,
        a.ExecuteDate AS BEGIN_DATE,
        a.CancelDate AS END_DATE,
        a.InsertTime AS CREATE_TIME,
        a.XGRQ AS UPDATE_TIME,
        a.JSID 
    FROM
        mf_investtargetcriterion a
        JOIN secumainall b ON a.InnerCode = b.InnerCode 
        AND a.InvestTarget = 7
        JOIN secumainall c ON a.TracedIndexCode = c.InnerCode
    where
        1=1
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_fund_benchmark():
    table = "fund_benchmark"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ID,
        b.SecuCode AS TICKER_SYMBOL,
        b.SecuAbbr AS SEC_SHORT_NAME,
        c.SecuCode AS IDX_SYMBOL,
        c.SecuAbbr AS IDX_SHORT_NAME,
        a.MinimumInvestRatio AS WEIGHT,
        a.IfExecuted AS IS_EXE,
        a.ExecuteDate AS BEGIN_DATE,
        a.CancelDate AS END_DATE,
        a.InsertTime AS CREATE_TIME,
        a.XGRQ AS UPDATE_TIME,
        a.JSID 
    FROM
        mf_investtargetcriterion a
        JOIN secumainall b ON a.InnerCode = b.InnerCode
        JOIN secumainall c ON a.TracedIndexCode = c.InnerCode 
    WHERE
        1 = 1 
        AND a.InvestTarget = 90
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_index_tag():
    table = "index_tag"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
        SELECT
            a.ID,
            b.SecuCode AS TICKER_SYMBOL,
            b.SecuAbbr AS SEC_SHORT_NAME,
            a.`Level` AS 'LEVEL',
            a.EffectiveDate AS BEGIN_DATE,
            a.CancelDate AS END_DATE,
            a.IfEffected AS IS_EXE,
            a.FirstTagName AS FIRST_TAG,
            a.SecTagName AS SEC_TAG,
            a.ThirdTagName AS THIRD_TAG,
            a.InsertTime AS CREATE_TIME,
            a.UpdateTime AS UPDATE_TIME,
            a.JSID 
        FROM
            index_tagchange a
            JOIN secumainall b ON a.IndexCode = b.InnerCode
        where
            1=1
            and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_global_index_eod_jy():
    table = "global_index_eod_jy"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ID,
        b.SecuCode AS TICKER_SYMBOL,
        a.TradingDay AS TRADE_DT,
        a.PrevClosePrice AS S_DQ_PRECLOSE,
        a.ClosePrice AS S_DQ_CLOSE,
        a.OpenPrice AS S_DQ_OPEN,
        a.LowPrice AS S_DQ_LOW,
        a.HighPrice AS S_DQ_HIGH,
        a.ChangePCT AS S_DQ_PCTCHANGE,
        a.TurnoverValue AS S_DQ_AMOUNT,
        a.TurnoverVolume AS S_DQ_VOLUME,
        log( 1+a.ChangePCT / 100 ) * 100 AS LOG_RET,
        a.UpdateTime AS UPDATE_TIME,
        a.JSID 
    FROM
        qt_osindexquote a
        JOIN secumainall b ON a.IndexCode = b.InnerCode 
    WHERE
        1 = 1 
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_fund_related():
    table = "fund_related"
    max_jsid = get_local_max_jsid(table=table)
    query_sql = f"""
    SELECT
        a.ID,
        b.SecuCode AS TICKER_SYMBOL,
        c.SecuCode AS RELATED_TICKER_SYMBOL,
        a.CodeDefine,
        d.MS AS CodeDefineName,
        a.SndCodeDefine,
        e.MS AS SndCodeDefineName,
        a.StartDate,
        a.EndDate,
        a.IfEffected,
        a.Remarks,
        a.UpdateTime,
        a.JSID 
    FROM
        mf_coderelationshipnew a
        JOIN secumain b ON a.InnerCode = b.InnerCode
        JOIN secumain c ON c.InnerCode = a.RelatedInnerCode
        LEFT JOIN CT_SystemConst d ON a.CodeDefine = d.DM 
        AND d.LB = 1350
        LEFT JOIN CT_SystemConst e ON a.SndCodeDefine = e.DM 
        AND e.LB = 1350 
    WHERE
        1 = 1
        and a.JSID >= {max_jsid}
    """
    query_from_remote_upsert_into_local(
        query_sql=query_sql, table=table, query_db_conn=DB_CONN_JY, if_rename=0
    )


def update_derivatives_jy():
    update_func_list = [
        update_jy_tbales,
        update_aindex_sw_eod,
        update_fund_asset_own_jy,
        update_fund_info_jy,
        update_fund_holdings_q_jy,
        update_fund_holdings_jy,
        update_fund_holder_info,
        update_fund_manager_info_jy,
        update_fund_share_change_jy,
        update_fund_adj_nav,
        update_fund_nav_gr,
        update_fund_calmar_ratio,
        update_fund_fee_new,
        update_fund_holding_bond,
        update_fund_equ_trade,
        update_fund_income_statement,
        update_fund_balance_sheet,
        update_jy_indexquote,
        update_bond_chinabondindexquote,
        update_qt_interestrateindex,
        update_fund_stock_portfolio_change,
        update_fund_holdings_fund,
        update_ed_consumerpriceindex,
        update_fund_purchaseandredeemn,
        update_fund_type_jy,
        update_fund_tracking_idx,
        update_fund_benchmark,
        update_index_tag,
        update_global_index_eod_jy,
        update_fund_related,
        del_data,
    ]
    for i, func in enumerate(update_func_list, start=1):
        try:
            for _ in range(5):
                func()
                print(f"{func.__name__}完成写入")
                print("==" * 35)
                break
        except Exception as e:
            logging.error(f"失败{i}:{func.__name__},原因是:{e}")
            print(f"失败{i}:{func.__name__},原因是:{e}")
            print("!!" * 35)
            logging.info("!!" * 35)


if __name__ == "__main__":
    update_jy_tbales()
