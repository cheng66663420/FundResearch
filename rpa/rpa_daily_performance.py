import subprocess

import emoji
import pandas as pd
from watermarker.marker import add_mark

import data_functions.portfolio_data as pdf
import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.utils import kill_processes_containing
from wrapper.excel_wrapper import ExcelWrapper
from wrapper.wx_wrapper import WxWrapper


def get_dates(TRADE_DATE: str = None) -> pd.DataFrame:
    """
    获取日期

    Parameters
    ----------
    TRADE_DATE : str, optional
        日期, by default None

    Returns
    -------
    pd.DataFrame
        返回一个DataFrame，包含日期和日期名称
    """
    if TRADE_DATE is None:
        TRADE_DATE = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        START_DATE AS DATE,
        DATE_NAME 
    FROM
        portfolio_dates 
    WHERE
        1 = 1 
        AND END_DATE = '{TRADE_DATE}' 
        AND PORTFOLIO_NAME = 'ALL' UNION
    SELECT DATE
        ( '{TRADE_DATE}' ) AS DATE,
        '报告日' AS DATE_NAME UNION
    SELECT DATE
        ( '2023-11-22' ) AS DATE,
        '指增成立日' AS DATE_NAME
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_index_period_ret(ticker_symbol: str, start_date: str, end_date: str) -> float:
    """
    获取指数的期间收益率
    """
    index_close = dm.get_index_close(ticker_symbol, start_date, end_date)
    return (
        index_close.iloc[-1]["S_DQ_CLOSE"] / index_close.iloc[0]["S_DQ_CLOSE"] - 1
    ) * 100


def update_dates(trade_date: str = None, df: pd.DataFrame = None) -> pd.DataFrame:
    """
    更新日期表数据

    Parameters
    ----------
    trade_date : str, optional
        交易日, by default None
    df : pd.DataFrame, optional
        原有的数据, by default None

    Returns
    -------
    pd.DataFrame
         返回一个DataFrame，包含日期和日期名称
    """
    if trade_date is None:
        trade_date = LAST_TRADE_DT
    dates = get_dates(trade_date).set_index("DATE_NAME")
    dates_list = df.index.tolist()
    df["DATE"] = dates.loc[dates_list, "DATE"]
    for idx in df.index:
        for col in df.columns[1:]:
            df.loc[idx, col] = (
                get_index_period_ret(
                    ticker_symbol=col.split(".")[0],
                    start_date=df.loc[idx, "DATE"],
                    end_date=df["DATE"].max(),
                )
                * 0.95
            )
    return df


def rpa_daily_performance(trade_date: str = None):
    """
    基金投顾每日业绩RPA

    Parameters
    ----------
    trade_date : str, optional
        交易日, by default None
    """
    if trade_date is None:
        trade_date = LAST_TRADE_DT
    kill_processes_containing("WPS")
    with ExcelWrapper(
        "E:/基金投顾自动化/日报周报/模板/策略业绩日报v7.xlsx"
    ) as excel_handler:
        # 更新日期
        excel_handler.select_sheet("日期")
        df = excel_handler.get_data("A1", convert=pd.DataFrame, expand="table")
        dates = update_dates(trade_date=trade_date, df=df)
        excel_handler.write_dataframe(dates, if_write_header=True, if_write_index=True)
        # 更新业绩表现
        excel_handler.select_sheet("表现")
        perf_df = pdf.query_portfolio_daily_performance(trade_date)
        perf_df.fillna("--", inplace=True)
        excel_handler.write_dataframe(
            perf_df, if_write_header=True, if_write_index=False
        )
        excel_handler.save("E:/基金投顾自动化/日报周报/结果/策略业绩日报v7.xlsx")
        # 画图
        excel_handler.select_sheet("日报")
        excel_handler.save_as_image("E:/基金投顾自动化/结果/策略业绩日报v7.png")
        # 添加水印
        add_mark(
            file="E:/基金投顾自动化/结果/策略业绩日报v7.png",
            out="E:/基金投顾自动化/结果/",
            mark="仅限内部参考，不可对外发送!",
            opacity=0.2,
            angle=30,
            space=100,
            size=50,
            color="#DB7093",
        )


def rpa_daily_target_portfolio_performance():
    """
    目标盈每日收益表现的rpa
    """
    target_portfolio = pdf.monitor_target_portfolio()
    target_portfolio.fillna("--", inplace=True)
    kill_processes_containing("WPS")
    with ExcelWrapper(
        "E:/基金投顾自动化/日报周报/模板/目标盈日度监控.xlsx"
    ) as excel_handler:
        excel_handler.write_data(
            data=f"知己目标盈监控\n{LAST_TRADE_DT[:4]}年{LAST_TRADE_DT[4:6]}月{LAST_TRADE_DT[6:8]}日",
            start_column=1,
            start_row=1,
        )
        # 写入数据
        excel_handler.write_dataframe(
            df=target_portfolio,
            if_write_header=True,
            if_write_index=False,
            start_row=2,
            start_column=1,
        )
        row_num, col_num = target_portfolio.shape
        # 复制格式
        source_range = excel_handler.select_range(
            start_row=3, start_column=1, expand="right"
        )

        for row in range(3, row_num + 3):
            target_range = excel_handler.select_range(
                start_row=row, start_column=1, expand="right"
            )
            excel_handler.format_painter(source_range, target_range)

        # 判断止盈
        for idx, val in target_portfolio.iterrows():
            end_date = val["运营结束日期"]
            if end_date != "--":
                continue
            if_reach_target_ret = val["是否触发止盈"]
            if if_reach_target_ret == "触发":
                range_obj_temp = excel_handler.select_range(
                    start_row=idx + 3, start_column=1, expand="right"
                )
                excel_handler.set_cell_style(range_obj_temp, bg_color="#DB7093")

        # 设置日期格式
        range_obj = excel_handler.select_range("B:D")
        excel_handler.set_cell_style(range_obj, number_format="yyyy-m-d")

        # 保存
        excel_handler.save("E:/基金投顾自动化/日报周报/结果/目标盈日度监控.xlsx")

    with ExcelWrapper(
        "E:/基金投顾自动化/日报周报/结果/目标盈日度监控.xlsx"
    ) as excel_handler:
        excel_handler.save_as_image("E:/基金投顾自动化/结果/目标盈日度监控.png")

    add_mark(
        file="E:/基金投顾自动化/结果/目标盈日度监控.png",
        out="E:/基金投顾自动化/结果/",
        mark="仅限内部参考，不可对外发送!",
        opacity=0.15,
        angle=30,
        space=100,
        size=50,
        color="#DB7093",
    )


if __name__ == "__main__":
    for _ in range(10):
        try:
            rpa_daily_performance(LAST_TRADE_DT)
            rpa_daily_target_portfolio_performance()
            wx_robot = WxWrapper()
            mention_moble_list = wx_robot.get_mentioned_moble_list_by_name(
                ["陈娇君", "陆天琦", "陈恺寅"]
            )
            wx_robot.send_image(
                image_path="E:/基金投顾自动化/结果/目标盈日度监控.png",
            )
            wx_robot.send_image(
                image_path="E:/基金投顾自动化/结果/策略业绩日报v7.png",
            )
            wx_robot.send_text(
                content="#每日播报 仅供各位领导同事内部参考，请勿对客转发",
            )
            wx_robot.send_text(
                content=f"{emoji.emojize('❣')}今日基金投顾业绩播报与目标盈播报已送达,请注意查收{emoji.emojize('❣')}",
                mentioned_mobile_list=mention_moble_list,
            )
            break
        except Exception as e:
            print(e)
            kill_processes_containing("WPS")
