# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 13:29:30 2023
@author: lorrainewhx
"""
import os
import sys

rootPath = "D://CODE"
sys.path.append(rootPath)
import time
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from quant_utils.constant import (
    DB_CONN_JJTG_DATA,
    DB_CONN_JY_TEST,
    DB_CONN_WIND,
)


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


# 聚源数据库取全部基金风格数据（季频）
def get_style_component(report_date):
    sql = f"""
        SELECT
        	a.ReportDate AS REPORT_DATE,
        	b.SecuCode AS TICKER_SYMBOL,
        	a.SharesHoldingPETTM AS PE_TTM,
            a.LCHoldingWeight AS LARGE_WEIGHT,
            a.MCHoldingWeight AS MEDIUM_WEIGHT,
            a.SCHoldingWeight AS SMALL_WEIGHT,
            a.GHoldingWeight AS GROWTH_WEIGHT,
            a.VHoldingWeight AS VALUE_WEIGHT,
            a.BHoldingWeight AS BALANCE_WEIGHT
        FROM
        	MF_StkHoldingAna a
        	JOIN secumain b ON a.InnerCode = b.InnerCode
        WHERE ReportDate = '{report_date}'
    """
    return DB_CONN_JY_TEST.exec_query(sql)


# 本地库取全部基金及持股信息，此处需要持仓股和特征进行匹配
def fund_stock_holdings(date: str, level_1: str, level_2: str):
    sql = f"""
        SELECT
            a.REPORT_DATE,
            a.TICKER_SYMBOL as FUND_SYMBOL,
            c.HOLDING_TICKER_SYMBOL,
            c.HOLDING_SEC_SHORT_NAME,
            c.RATIO_IN_NA
        FROM
            `fund_type_own` a JOIN fund_info b on a.TICKER_SYMBOL = b.TICKER_SYMBOL
            JOIN fund_holdings c on b. FUND_ID = c.FUND_ID
        WHERE
            a.REPORT_DATE = '{date}'
            AND	c.REPORT_DATE= '{date}'
            AND a.LEVEL_1 = '{level_1}' 
            AND a.LEVEL_2 = '{level_2}'
            AND c.SECURITY_TYPE='E'
    """
    return DB_CONN_JJTG_DATA.exec_query(sql)


# 本地库取全部基金，此处直接匹配基金和对应风格数据
def get_all_fund(date: str, level_1: str):
    sql = f"""
        SELECT
            a.REPORT_DATE,
            a.TICKER_SYMBOL as FUND_SYMBOL
        FROM
            `fund_type_own` a 
        WHERE
            a.REPORT_DATE = '{date}'
            AND a.LEVEL_1 = '{level_1}'
    """
    return DB_CONN_JJTG_DATA.exec_query(sql)


# 计算基金质量得分
def cal_quality_score(report_date, level_1, level_2):
    # 取出质量得分
    df = get_quality_component(report_date)
    df["EST_ROE_SCORE"] = df["EST_ROE"].rank(pct=True) * 100
    df["ROE_TTM_SCORE"] = df["ROE_TTM"].rank(pct=True) * 100
    df["ROA_TTM_SCORE"] = df["ROA_TTM"].rank(pct=True) * 100
    df["TOTAL_SCORE"] = np.mean(df.iloc[:, 6:8], axis=1)

    # 取出目标基金所持有的全部股票和比例
    fund_stock_holding = fund_stock_holdings(report_date, level_1, level_2)

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
    return df_quality.groupby(by=["FUND_SYMBOL"])["RATIO_SCORE"].sum() / 100


def fund_style_score(report_date):
    fund_style_component = get_style_component(report_date)
    all_fund = get_all_fund(date=report_date, level_1="主动权益")
    all_fund_style = pd.merge(
        all_fund,
        fund_style_component,
        left_on="FUND_SYMBOL",
        right_on="TICKER_SYMBOL",
        how="left",
    ).drop_duplicates()
    all_fund_style.drop(columns=["REPORT_DATE_y", "TICKER_SYMBOL"], inplace=True)
    df_numeric = (all_fund_style.loc[:, "PE_TTM":"BALANCE_WEIGHT"]).apply(
        lambda x: pd.to_numeric(x)
    )
    all_fund_style = pd.concat([all_fund_style.iloc[:, :2], df_numeric], axis=1)

    def size_style(x, y):
        if x >= 0.7:
            a = "LARGE"
        elif y >= 0.7:
            a = "SMALL"
        else:
            a = "MEDIUM"
        return a

    def growth_style(x, y):
        if x >= 0.7:
            a = "GROWTH"
        elif y >= 0.7:
            a = "VALUE"
        else:
            a = "BALANCE"
        return a

    all_fund_style["SMALL_LARGE"] = all_fund_style.apply(
        lambda x: size_style(x["LARGE_WEIGHT"], x["SMALL_WEIGHT"]), axis=1
    )
    all_fund_style["GROWTH_VALUE"] = all_fund_style.apply(
        lambda x: growth_style(x["GROWTH_WEIGHT"], x["VALUE_WEIGHT"]), axis=1
    )
    return all_fund_style


# fund_quality_score = cal_quality_score("2022-06-30", "主动权益", "A股主题")
fund_style_score = fund_style_score(report_date="2022-06-30")
print(fund_style_score)
