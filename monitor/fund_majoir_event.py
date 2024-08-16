import datetime
import os
import sys
from functools import reduce

import pandas as pd

import quant_utils.data_moudle as dm
from data_functions.portfolio_data import (
    query_portfolio_daily_evalution,
    query_portfolio_daily_performance,
)
from portfolio.portfolio_risk_management import check_portfolio
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_JY
from quant_utils.send_email import MailSender

# rootPath = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
# sys.path.append(rootPath)


today = dm.get_now()
TODAY = dm.get_now()
LAST_TRADE_DT = (
    dm.get_recent_trade_dt(TODAY)
    if dm.if_trade_dt(TODAY)
    else dm.offset_trade_dt(TODAY, 0)
)

RECIEVER_LIST = [
    "569253615@qq.com",
    "chentiancheng@xyzq.com.cn",
    "chenjiaojun@xyzq.com.cn",
    "chenkaiyin@xyzq.com.cn",
    "lutianqi@xyzq.com.cn",
    "shouyining@xyzq.com.cn",
    "weili@xyzq.com.cn",
    "zhengkedong@xyzq.com.cn",
    "chengyuyang@xyzq.com.cn",
]

RECIEVER_LIST_ANALYST = [
    "569253615@qq.com",
    "chentiancheng@xyzq.com.cn",
    "chenjiaojun@xyzq.com.cn",
    "chenkaiyin@xyzq.com.cn",
    "lutianqi@xyzq.com.cn",
]


def get_fund_major_events(publish_date: str = None):
    if publish_date is None:
        today = datetime.datetime.now().strftime("%Y%m%d")
        publish_date = dm.offset_trade_dt(today, 1)

    query_sql = f"""
    SELECT
        b.SecuCode AS TICKER_SYMBOL,
        b.SecuAbbr as SEC_SHORT_NAME,
        a.InfoPublDate,
        f.TypeName,
        a.InfoTitle
    FROM
        mf_nottextannouncement a
        JOIN secumain b ON a.InnerCode = b.InnerCode 
        join mf_annclassifi c on c.NotTextAnnID = a.ID 
        join lc_announcestru e on e.TypeCode = c.Level2
        join lc_announcestru f on f.TypeCode = c.Level3
    WHERE
        1 = 1 
        AND a.InfoPublDate >= '{publish_date}'
    """
    annouce_df = DB_CONN_JY.exec_query(query_sql)
    ticker_query = """
    SELECT
        a.PORTFOLIO_NAME,
        a.TICKER_SYMBOL 
    FROM
        `view_portfolio_holding_new` a
        JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
    WHERE
        1 = 1 
        and b.IF_LISTED = 1
        and b.DELISTED_DATE is NULL
    ORDER BY
        b.ORDER_ID
    """
    ticker_df = DB_CONN_JJTG_DATA.exec_query(ticker_query)

    return ticker_df.merge(annouce_df, on=["TICKER_SYMBOL"], how="inner")


def get_fund_dividend_events(publish_date: str = None):
    if publish_date is None:
        today = datetime.datetime.now().strftime("%Y%m%d")
        publish_date = dm.offset_trade_dt(today, 1)
    query_sql = f"""
    SELECT
        b.SecuCode AS TICKER_SYMBOL,
        b.SecuAbbr AS SEC_SHORT_NAME,
        a.InfoPublDate as '公告日',
        a.ExRightDate as '除权除息日',
        a.ExecuteDate  as '分红日',
        a.ActualRatioAfterTax 
    FROM
        MF_Dividend a
        JOIN secumain b ON a.InnerCode = b.InnerCode 
    WHERE
        1 = 1 
        AND a.InfoPublDate >= '{publish_date}'
    """
    annouce_df = DB_CONN_JY.exec_query(query_sql)
    ticker_query = """
    SELECT
        a.PORTFOLIO_NAME,
        a.TICKER_SYMBOL 
    FROM
        `view_portfolio_holding_new` a
        JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
    WHERE
        b.IF_LISTED = 1 
    ORDER BY
        b.ID
    """
    ticker_df = DB_CONN_JJTG_DATA.exec_query(ticker_query)

    return ticker_df.merge(annouce_df, on=["TICKER_SYMBOL"], how="inner")


def monitor_target_portfolio():
    query_sql = """
    SELECT
        a.*,
        b.`累计收益率(止盈后)`,
        b.`年化收益率(止盈后)`,
        c.PORTFOLIO_VALUE AS '最大回撤',
        d.TEMPERATURE AS '权益温度',
        e.`债市温度计` 
    FROM
        `view_mointor_target_portfolio` a
        JOIN view_mointor_target_portfolio_accumulated b ON a.`组合名称` = b.`组合名称`
        LEFT JOIN portfolio_derivatives_performance c ON c.END_DATE = b.`交易日` 
        AND c.TICKER_SYMBOL = a.`组合名称` 
        AND c.`CYCLE` = '成立日' 
        AND c.`INDICATOR` = '最大回撤'
        JOIN view_temperature_stock d ON d.END_DATE = a.`运作起始日` 
        AND d.TICKER_SYMBOL = '000985'
        JOIN view_temperature_bond e ON e.`日期` = d.END_DATE 
    WHERE
        1 = 1 
    ORDER BY
        `是否年化`,
        `运营结束日期`,
        `运作起始日`,
        `是否触发止盈` DESC
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def monitor_north_money() -> pd.DataFrame:
    """
    监控北向资金流向

    Returns
    -------
    pd.DataFrame
        最近5日北向资金流向
    """
    query_sql = """
    WITH a AS ( SELECT * FROM jy_local.LC_ZHSCTradeStat WHERE ReportPeriod = 29 UNION SELECT * FROM jy_local.LC_SHSCTradeStat WHERE ReportPeriod = 29 ),
    b AS (
        SELECT
            EndDate,
            sum( TradValNetSum ) / 100000000 AS TradValNetSum 
        FROM
            a 
        WHERE
            TradingType IN ( 1, 3 ) 
        GROUP BY
            EndDate 
        ORDER BY
            EndDate 
        ) SELECT
        EndDate,
        TradValNetSum,
        avg( TradValNetSum ) over ( ORDER BY EndDate rows 4 preceding ) AS ma_5,
        avg( TradValNetSum ) over ( ORDER BY EndDate rows 19 preceding ) AS ma_20,
        avg( TradValNetSum ) over ( ORDER BY EndDate rows 59 preceding ) AS ma_60
    FROM
        b 
    ORDER BY
        EndDate desc
        LIMIT 5
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def send_portfolio_daily_performance(end_date: str):
    portfolio_performance = query_portfolio_daily_performance(end_date)
    if not portfolio_performance.empty:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"!!!【组合净值监控报表】{end_date}",
            df_to_mail=portfolio_performance,
        )
        mail_sender.send_mail()


def send_portfolio_daily_evalution(end_date: str):
    portfolio_evalution = query_portfolio_daily_evalution(end_date)
    if not portfolio_evalution.empty:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"!!!【组合评价表】{end_date}",
            df_to_mail=portfolio_evalution,
        )

        # 如果是周末则发给所有人
        if dm.if_period_end(end_date, period="WEEK"):
            mail_sender.send_mail()

        # 不是周末则定向发
        else:
            mail_sender.send_mail(RECIEVER_LIST)


def send_fund_major_events(end_date):
    df = get_fund_major_events(end_date)
    if not df.empty:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"!!!【重大事件监控】近期持仓大事件{today}",
            df_to_mail=df,
        )

    else:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"【重大事件监控】近期持仓大事件{today}: 无事发生",
            df_to_mail=df,
        )

    mail_sender.send_mail(
        receivers=[
            "569253615@qq.com",
            "chentiancheng@xyzq.com.cn",
        ]
    )


def send_fund_dividend_events(end_date):
    # 分红监控
    df = get_fund_dividend_events(end_date)

    if not df.empty:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"!!!【分红】{today}",
            df_to_mail=df,
        )

    else:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"【分红】{today}: 无事发生",
            df_to_mail=df,
        )

    mail_sender.send_mail(receivers=RECIEVER_LIST_ANALYST)


def send_target_portfolio(receivers: list = None):
    # 目标盈监控
    if receivers is None:
        receivers = RECIEVER_LIST
    df = monitor_target_portfolio()

    def highlight_first_row(s):
        result_list = []
        for _ in range(len(s)):
            if (
                s["组合状态"] == "运作中(可止盈)"
                and s["是否止盈"] == "未止盈"
                and s["是否触发止盈"] == "触发"
            ):
                result_list.append(
                    "background-color: seashell; color: #DB7093; text-align: center;"
                )
            else:
                result_list.append("text-align: center; background-color: #F8F8FF")
        return result_list

    df1 = df.style.format(na_rep="--").apply(highlight_first_row, axis=1)
    if not df.empty:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"!!!【目标盈监控】{today}",
            df_to_mail=df1,
        )

    else:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"【目标盈监控】{today}: 无事发生",
            df_to_mail=df,
        )

    mail_sender.send_mail(receivers=receivers)


def send_north_monyey():
    df = monitor_north_money()
    if not df.empty:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"!!!【北向资金监控】{today}",
            df_to_mail=df,
        )
        mail_sender.send_mail(receivers=["569253615@qq.com"])


def send_portfolio_risk_management():
    df = check_portfolio()
    df = df.query("校验结果 == '失败'")
    if not df.empty:
        mail_sender = MailSender()
        mail_sender.message_config_dataframe(
            from_name="进化中的ChenGPT_0.1",
            subject=f"!!!【组合风险管理监控】{today}",
            df_to_mail=df,
        )
        mail_sender.send_mail(
            receivers=[
                "569253615@qq.com",
                "weili@xyzq.com.cn",
                "zhengkedong@xyzq.com.cn",
                "chentiancheng@xyzq.com.cn",
                "shouyining@xyzq.com.cn",
                "chengyuyang@xyzq.com.cn",
            ]
        )


if __name__ == "__main__":
    # print(get_fund_dividend_events("20231210"))
    if dm.if_trade_dt(today):
        publish_date = dm.offset_trade_dt(today, 2)

        send_fund_major_events(publish_date)
        send_fund_dividend_events(publish_date)
        send_target_portfolio()

        send_portfolio_daily_performance(LAST_TRADE_DT)
        send_portfolio_daily_evalution(LAST_TRADE_DT)
        send_portfolio_risk_management()
        send_north_monyey()
