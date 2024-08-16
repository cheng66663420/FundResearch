import emoji
import pandas as pd
from watermarker.marker import add_mark

import data_functions.portfolio_data as pdf
import quant_utils.data_moudle as dm
from portfolio.portfolio_product_contribution import (
    get_portfolios_products_contribution,
)
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.utils import kill_processes_containing
from wrapper.excel_wrapper import ExcelWrapper
from wrapper.wx_wrapper import WxWrapper

TEMPLATE_PATH = "E:/基金投顾自动化/日报周报/模板/绩效分析.xlsx"
SAVE_PATH = "E:/基金投顾自动化/日报周报/结果/绩效分析.xlsx"


def rpa_portfolio_contribution(
    portfolio_list: list = None,
    start_date: str = None,
    end_date: str = None,
    level_num: int = 1,
):
    kill_processes_containing("WPS")
    abs_ret, alpha_ret, sum_ret = get_portfolios_products_contribution(
        start_date=start_date,
        end_date=end_date,
        portfolio_list=portfolio_list,
        level_num=level_num,
    )

    with ExcelWrapper(TEMPLATE_PATH) as excel_handler:
        excel_handler.select_sheet("累计收益")
        excel_handler.write_dataframe(
            abs_ret, if_write_index=False, if_write_header=True
        )
        temp_source_range = excel_handler.select_range(
            start_row=2,
            start_column=1,
            expand="right",
        )

        temp_target_range = excel_handler.select_range(
            start_row=2, start_column=1, expand="table"
        )

        excel_handler.format_painter(
            source_range=temp_source_range, target_range=temp_target_range
        )

        excel_handler.select_sheet("超额收益")
        excel_handler.write_dataframe(
            alpha_ret, if_write_index=False, if_write_header=True
        )
        temp_source_range = excel_handler.select_range(
            start_row=2,
            start_column=1,
            expand="right",
        )

        temp_target_range = excel_handler.select_range(
            start_row=2, start_column=1, expand="table"
        )
        excel_handler.format_painter(
            source_range=temp_source_range, target_range=temp_target_range
        )

        excel_handler.select_sheet("总收益")
        excel_handler.write_dataframe(
            sum_ret, if_write_index=False, if_write_header=True
        )
        temp_source_range = excel_handler.select_range(
            start_row=2,
            start_column=1,
            expand="right",
        )

        temp_target_range = excel_handler.select_range(
            start_row=2, start_column=1, expand="table"
        )
        excel_handler.format_painter(
            source_range=temp_source_range, target_range=temp_target_range
        )
        excel_handler.save(SAVE_PATH)

    robot = WxWrapper()
    mentioned_mobile_list = robot.get_mentioned_moble_list_by_name(
        ["陈娇君", "陈恺寅", "陆天琦"]
    )
    robot.send_file(SAVE_PATH)
    robot.send_text(
        content=f"{emoji.emojize('❣')}组合业绩绩效分析已生成,请查收{emoji.emojize('❣')}",
        mentioned_mobile_list=mentioned_mobile_list,
    )


if __name__ == "__main__":
    if dm.if_period_end(LAST_TRADE_DT):
        start_date = dm.offset_period_trade_dt(LAST_TRADE_DT, -1, period="w")
        start_date = dm.offset_trade_dt(start_date, 1)
        rpa_portfolio_contribution(start_date=start_date, end_date=LAST_TRADE_DT)

    if dm.if_period_end(LAST_TRADE_DT, period="MONTH"):
        start_date = dm.offset_period_trade_dt(LAST_TRADE_DT, -1, period="m")
        start_date = dm.offset_trade_dt(start_date, 1)
        rpa_portfolio_contribution(start_date=start_date, end_date=LAST_TRADE_DT)
    # rpa_portfolio_contribution(start_date="20240701", end_date="20240705")
