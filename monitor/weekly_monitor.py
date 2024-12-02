import datetime
import math
import os
import sys

import numpy as np
import pandas as pd

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.performance import Performance
from quant_utils.send_email import MailSender

# rootPath = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
# sys.path.append(rootPath)


trade_dt = "2014-12-30"

dates_query = f"""
select 
TRADE_DT
from 
md_tradingdaynew 
where 
SECU_MARKET = 83 
AND (IF_WEEK_END = 1 or IF_MONTH_END = 1)
and TRADE_DT >= '{trade_dt}'
"""
dates_df = DB_CONN_JJTG_DATA.exec_query(dates_query)

query_sql = """
SELECT
	a.END_DATE as TRADE_DT,
	a.TEMPERATURE AS 'STOCK_TEMPERATURE',
	b.`债市温度计` AS BOND_TEMPERATURE 
FROM
	view_temperature_stock a
	JOIN view_temperature_bond b ON a.END_DATE = b.`日期` 
WHERE
	1 = 1 
	AND a.`TICKER_SYMBOL` = '000985' 
	AND a.START_DATE >= '20141231' 
ORDER BY
	a.END_DATE
"""
term_df = DB_CONN_JJTG_DATA.exec_query(query_sql)

all_temp = term_df.merge(dates_df)[
    ["TRADE_DT", "STOCK_TEMPERATURE", "BOND_TEMPERATURE"]
]

all_temp["STOCK_PORTATION"] = all_temp["STOCK_TEMPERATURE"].apply(
    lambda s: (
        0.5
        if 30 < s < 70
        else (
            0.5 + math.ceil((30 - s) / 5) * 0.05
            if s < 30
            else 0.5 - math.ceil((s - 70) / 5) * 0.05
        )
    )
)
all_temp["CASH_PORTATION"] = all_temp.apply(
    lambda s: (
        1 - s["STOCK_PORTATION"]
        if s["STOCK_TEMPERATURE"] > 70 and s["BOND_TEMPERATURE"] > 90
        else ((1 - s["STOCK_PORTATION"]) * 0.5 if s["BOND_TEMPERATURE"] > 90 else 0)
    ),
    axis=1,
)
all_temp["BOND_PLUS_PORTATION"] = (
    1 - all_temp["CASH_PORTATION"] - all_temp["STOCK_PORTATION"]
)
# all_temp[["STOCK_PORTATION", "CASH_PORTATION", "BOND_PLUS_PORTATION"]] = all_temp[
#     ["STOCK_PORTATION", "CASH_PORTATION", "BOND_PLUS_PORTATION"]
# ].shift(1).fillna(0)

query_sql = """
SELECT
	TradingDay AS TRADE_DT,
	SecuCode AS TICKER_SYMBOL,
	ClosePrice AS S_DQ_CLOSE 
FROM
	jy_indexquote a 
WHERE
	1 = 1 
	AND SecuCode IN (
	'930950',
	'930609') UNION
    SELECT
	TRADE_DT,
	TICKER_SYMBOL,
	S_DQ_CLOSE 
FROM
	fund_index_eod a 
WHERE
	1 = 1 
	AND TICKER_SYMBOL IN (
	'930950',
	'930609') UNION
SELECT
	`bond_chinabondindexquote`.`TRADE_DT` AS `TRADE_DT`,
	`bond_chinabondindexquote`.`TICKER_SYMBOL` AS `TICKER_SYMBOL`,
	`bond_chinabondindexquote`.`S_DQ_CLOSE` AS `S_DQ_CLOSE` 
FROM
	`bond_chinabondindexquote` 
WHERE
	((
			1 = 1 
			) 
	AND ( `bond_chinabondindexquote`.`TICKER_SYMBOL` = 'CBA00211' )) UNION
SELECT
	`qt_interestrateindex`.`TRADE_DT` AS `TRADE_DT`,
	'B00009' AS `TICKER_SYMBOL`,
	`qt_interestrateindex`.`IndexDD` AS `S_DQ_CLOSE` 
FROM
	`qt_interestrateindex` 
WHERE
	( 1 = 1 )
"""
nav_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
nav_df = (
    nav_df.pivot_table(index="TRADE_DT", columns="TICKER_SYMBOL", values="S_DQ_CLOSE")
    .reset_index()
    .merge(dates_df)
    .set_index("TRADE_DT")
    .pct_change()
    .fillna(0)
)

nav_df["CASH"] = nav_df["CBA00211"] * 0.25 + nav_df["B00009"] * 0.75
nav_df["STOCK"] = nav_df["930950"]
nav_df["BOND_PLUS"] = nav_df["930950"] * 0.1 + nav_df["930609"] * 0.9
strategy_ret = nav_df[["CASH", "STOCK", "BOND_PLUS"]].reset_index()

all_temp = all_temp.merge(strategy_ret)


all_temp["PORTFOLIO"] = (
    all_temp["CASH_PORTATION"].shift(1).fillna(0) * all_temp["CASH"]
    + all_temp["STOCK_PORTATION"].shift(1).fillna(0) * all_temp["STOCK"]
    + all_temp["BOND_PLUS_PORTATION"].shift(1).fillna(0) * all_temp["BOND_PLUS"]
)

all_temp["偏股基金"] = (all_temp["STOCK"] + 1).cumprod()
all_temp["周度推动"] = (all_temp["PORTFOLIO"] + 1).cumprod()
all_temp = all_temp.set_index("TRADE_DT")

port_perf = Performance(all_temp["周度推动"]).stats().T
port_perf["组合"] = "周度推动"
bench_perf = Performance(all_temp["偏股基金"]).stats().T
bench_perf["组合"] = "偏股基金"

perf_stat = pd.concat([port_perf, bench_perf])

with pd.ExcelWriter("D:/周报推动.xlsx") as w:
    all_temp.to_excel(w, sheet_name="净值")
    perf_stat.to_excel(w, sheet_name="绩效", index=False)

date = dm.get_recent_trade_dt(dm.get_now())
print(date)
if dm.if_period_end(date, period="WEEK"):
    mail_sender = MailSender()
    mail_sender.message_config(
        from_name="进化中的ChenGPT_0.1",
        subject=f"【周度推动】净值及绩效数据{date}",
        file_path="D:/周报推动.xlsx",
        content="详情请见附件",
    )
    # mail_sender.send_mail()
