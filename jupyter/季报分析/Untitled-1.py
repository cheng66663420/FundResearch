# %%
import sys

sys.path.append("d:\\FundResearch\\")
import pandas as pd
from dateutil.parser import parse

import quant_utils.data_moudle as dm
from quant_utils.constant import (
    BARRA_SW21_FACTOR_NAME_DICT,
    DB_CONN_DATAYES,
    DB_CONN_JJTG_DATA,
)

# %% [markdown]
# 季报2固收及固收加杠杆券种权益仓位变动

# %%
query_sql = """
WITH t ( REPORT_DATE, LEVEL1, LEVEL2, F_PRT_BONDTONAV, F_PRT_COVERTBONDTONAV, F_PRT_INTERESETBONDTONAV, F_PRT_CREDITBONDTONAV, EQUITY_IN_NA, HK_in_na ) AS (
	SELECT
	CASE
			grouping ( a.REPORT_DATE ) 
			WHEN 1 THEN
			'总计' ELSE a.REPORT_DATE 
		END,
	CASE
			grouping ( b.LEVEL_1 ) 
			WHEN 1 THEN
			'总计' ELSE b.LEVEL_1 
		END,
	CASE
			grouping ( b.LEVEL_2 ) 
			WHEN 1 THEN
			'--' ELSE b.LEVEL_2 
		END,
		avg( a.F_PRT_BONDTONAV ),
		avg( a.F_PRT_COVERTBONDTONAV ),
		avg( `a`.`F_PRT_GOVBONDTONAV` + `a`.`F_PRT_LOCALGOVBONDTONAV` + `a`.`F_PRT_CTRBANKBILLTONAV` + `a`.`F_PRT_POLIFINANBDTONAV` ),
		avg( `a`.`F_PRT_BONDTONAV` - `a`.`F_PRT_COVERTBONDTONAV` - `a`.`F_PRT_GOVBONDTONAV` - `a`.`F_PRT_LOCALGOVBONDTONAV` - `a`.`F_PRT_CTRBANKBILLTONAV` - a.F_PRT_POLIFINANBDTONAV ),
		avg( a.EQUITY_MARKET_VALUE / a.NET_ASSET * 100 ),
		avg( a.F_PRT_HKSTOCKVALUETONAV ) 
	FROM
		fund_asset_own a
		JOIN fund_type_own b ON b.REPORT_DATE = a.REPORT_DATE 
		AND a.TICKER_SYMBOL = b.TICKER_SYMBOL 
	WHERE
		b.LEVEL_1 IN ( '固收', '固收+' ) 
	GROUP BY
		a.REPORT_DATE,
		b.LEVEL_1,
		b.LEVEL_2 WITH ROLLUP 
	HAVING
		( grouping ( a.REPORT_DATE ) <> 1 ) 
	) SELECT
	REPORT_DATE as '报告期',
	level1 as '知己内部分类:一级',
	level2 as '知己内部分类:二级',
	round(F_PRT_BONDTONAV,2) + round(EQUITY_IN_NA,2) as '杠杆',
	round(F_PRT_COVERTBONDTONAV,2) as '可转债占净值',
	round(F_PRT_INTERESETBONDTONAV,2) as '利率债占净值',
	round(F_PRT_CREDITBONDTONAV,2) as '信用债占净值',
	round(EQUITY_IN_NA,2) as '权益占净值',
    round(EQUITY_IN_NA + 0.5*F_PRT_COVERTBONDTONAV,2) as '风险暴露'
FROM
	t 
WHERE
	level2 IS NOT NULL 
	and level1 != '总计' and level2 != '--'
	and report_date >= '20191231'
ORDER BY
	level1,
	level2,
	REPORT_DATE;
"""
df = DB_CONN_JJTG_DATA.exec_query(query_sql)
df_leverage = df.pivot_table(columns="知己内部分类:二级", index="报告期", values="杠杆")
df_insteret = df.pivot_table(
    columns="知己内部分类:二级", index="报告期", values="利率债占净值"
)
df_credit = df.pivot_table(
    columns="知己内部分类:二级", index="报告期", values="信用债占净值"
)
df_stock = df.pivot_table(
    columns="知己内部分类:二级", index="报告期", values="权益占净值"
)
df_risk_exposure = df.pivot_table(
    columns="知己内部分类:二级", index="报告期", values="风险暴露"
)

# %%
df_risk_exposure

# %% [markdown]
# 权益基金1仓位

# %%
query_sql = """
WITH t ( REPORT_DATE, level1,level2,EQUITY_IN_NA, HK_in_na, A_in_na ) AS (
	SELECT
	CASE
			grouping ( a.REPORT_DATE ) 
			WHEN 1 THEN
			'总计' ELSE a.REPORT_DATE 
		END,
	CASE
			grouping ( b.LEVEL_1 ) 
			WHEN 1 THEN
			'总计' ELSE b.LEVEL_1 
		END,
	CASE
			grouping ( b.LEVEL_2 ) 
			WHEN 1 THEN
			'--' ELSE b.LEVEL_2 
		END,
		avg( a.EQUITY_MARKET_VALUE / a.NET_ASSET * 100 ),
		avg( a.F_PRT_HKSTOCKVALUETONAV ),
		avg( a.EQUITY_MARKET_VALUE / a.NET_ASSET * 100 - a.F_PRT_HKSTOCKVALUETONAV)
	FROM
		fund_asset_own a
		JOIN fund_type_own b ON b.REPORT_DATE = a.REPORT_DATE 
		AND a.TICKER_SYMBOL = b.TICKER_SYMBOL 
	WHERE
		b.LEVEL_1 = '主动权益' 
	GROUP BY
		a.REPORT_DATE,
		b.LEVEL_1,
		b.LEVEL_2 WITH ROLLUP 
	HAVING
		( grouping ( a.REPORT_DATE ) <> 1 ) 
	) SELECT
	REPORT_DATE as '报告期',
	level1 as '知己内部分类:一级',
	level2 as '知己内部分类:二级',
	round(EQUITY_IN_NA,2) as '仓位',
	round(HK_in_na,2) as '港股仓位',
	round(A_in_na,2) as 'A股仓位'
FROM
	t 
WHERE
	level2 IS NOT NULL and level1 != '总计'
	and report_date >= '20191231'
ORDER BY
	level1,
	level2,
	REPORT_DATE;
"""
df = DB_CONN_JJTG_DATA.exec_query(query_sql)

# %%
df

# %% [markdown]
# 权益基金2-前十大占比

# %%
query_sql = """
WITH t AS (
	SELECT
		REPORT_DATE,
		FUND_ID,
		HOLDING_TICKER_SYMBOL,
		HOLDING_SEC_SHORT_NAME,
		RATIO_IN_NA,
        MARKET_VALUE,
		ROW_NUMBER() over ( PARTITION BY REPORT_DATE, FUND_ID, SECURITY_TYPE ORDER BY RATIO_IN_NA DESC ) AS NUMBER_N 
	FROM
		fund_holdings 
	WHERE
		REPORT_DATE >= '20141231' 
		AND SECURITY_TYPE = 'E' 
	ORDER BY
		REPORT_DATE,
		FUND_ID,
		NUMBER_N 
	) SELECT
	b.TICKER_SYMBOL,
	b.SEC_SHORT_NAME,
	t.*,
	d.PREV_TRADE_DATE,
	c.LEVEL_1,
	c.LEVEL_2
FROM
	t
	JOIN fund_info b ON b.FUND_ID = t.FUND_ID and b.IS_MAIN = 1
	JOIN fund_type_own c ON c.TICKER_SYMBOL = b.TICKER_SYMBOL 
	AND c.report_date = t.report_date 
	AND c.LEVEL_1 = '主动权益'
	join md_trade_cal d on d.CALENDAR_DATE = t.REPORT_DATE and d.EXCHANGE_CD = 'XSHG'
WHERE t.NUMBER_N <= 10 
"""
df = DB_CONN_JJTG_DATA.exec_query(query_sql)

# %%
df_sum = (
    df.groupby(
        by=[
            "PREV_TRADE_DATE",
            "FUND_ID",
            "TICKER_SYMBOL",
            "SEC_SHORT_NAME",
            "LEVEL_1",
            "LEVEL_2",
        ]
    )
    .sum()
    .reset_index()
)
df_top10_sum_mean = (
    df_sum.groupby(by=["PREV_TRADE_DATE", "LEVEL_1", "LEVEL_2"])["RATIO_IN_NA"]
    .mean()
    .reset_index()
)
df_top10_sum_mean1 = df_top10_sum_mean.pivot_table(
    index="PREV_TRADE_DATE", values="RATIO_IN_NA", columns="LEVEL_1"
)

# %%
df_sum_mean = (
    df.groupby(
        by=["PREV_TRADE_DATE", "HOLDING_TICKER_SYMBOL", "HOLDING_SEC_SHORT_NAME"]
    )
    .sum()
    .reset_index()
    .sort_values(by=["PREV_TRADE_DATE", "RATIO_IN_NA"], ascending=False)
)

df_sum_mean["rank"] = df_sum_mean.groupby(by=["PREV_TRADE_DATE"])["MARKET_VALUE"].rank(
    ascending=False
)

df_sum = (
    df_sum_mean.groupby(by=["PREV_TRADE_DATE"])["MARKET_VALUE"]
    .sum()
    .reset_index()
    .rename(columns={"MARKET_VALUE": "MARKET_VALUE_SUM"})
)
df_sum_mean = df_sum_mean.merge(df_sum)
df_sum_mean["比例"] = (
    df_sum_mean["MARKET_VALUE"] / df_sum_mean["MARKET_VALUE_SUM"]
) * 100

# %% [markdown]
# 权益基金-公募基金前50/100大占比

# %%
df_sum_mean.query("rank <= 100").groupby("PREV_TRADE_DATE")["比例"].sum()

# %% [markdown]
# 权益基金-前十大风格

# %%
barra_exposure_sql = """
SELECT
	a.*
FROM
	dy1d_exposure_cne6 a
	join (SELECT DISTINCT
		quarter_end_date 
	FROM
		md_trade_cal 
WHERE
	EXCHANGE_CD = 'XSHG' 
	AND quarter_end_date BETWEEN '20141230' 
AND '20221230') t on a.TRADE_DATE = t.quarter_end_date
"""
barra_exposure = DB_CONN_DATAYES.exec_query(barra_exposure_sql).drop(columns=["ID"])
barra_exposure["TRADE_DATE"] = barra_exposure["TRADE_DATE"].apply(
    lambda s: parse(s).strftime("%Y-%m")
)

# %%
barra_exposure = barra_exposure.dropna(axis=1)

# %%
barra_exposure.loc[:, "BETA":"COUNTRY"] = barra_exposure.loc[
    :, "BETA":"COUNTRY"
].astype("float")

# %%
df_sum_mean["PREV_TRADE_DATE1"] = df_sum_mean["PREV_TRADE_DATE"].apply(
    lambda s: s.strftime("%Y-%m")
)

# %%
(
    df_sum_mean.merge(
        barra_exposure,
        left_on=["HOLDING_TICKER_SYMBOL", "PREV_TRADE_DATE1"],
        right_on=["TICKER_SYMBOL", "TRADE_DATE"],
    )
    .groupby(by="PREV_TRADE_DATE1")
    .mean()
    .rename(columns=BARRA_SW21_FACTOR_NAME_DICT)
    .to_excel("d:/barra.xlsx")
)

# %%
df_barra = df_sum_mean.merge(
    barra_exposure,
    left_on=["HOLDING_TICKER_SYMBOL", "PREV_TRADE_DATE1"],
    right_on=["TICKER_SYMBOL", "TRADE_DATE"],
)

# %%
col = df_barra.columns

# %%
for column in col[13:-2]:
    df_barra.loc[:, column] = df_barra.loc[:, column] * df_barra.loc[:, "比例"]

# %%
df1 = df_barra.groupby(by="PREV_TRADE_DATE1").sum()

# %%
for column in col[13:-2]:
    df1.loc[:, column] = df1.loc[:, column] / df1.loc[:, "比例"]

# %%
df1.rename(columns=BARRA_SW21_FACTOR_NAME_DICT).to_excel("D:/barra.xlsx")

# %%
col[13]
