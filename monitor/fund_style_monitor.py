import datetime
import logging
import os
import sys

import pandas as pd
from joblib import Parallel, delayed

import quant_utils.data_moudle as dm
from fund_db.fund_db_cal_func import (
    cal_fund_inner_alpha_model,
    cal_fund_inner_alpha_performance,
)
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.send_email import MailSender
from quant_utils.utils import cal_exposure

cons_dict = {
    "style": (
        {"type": "eq", "fun": lambda A: 1 - sum(A[:-1])},
        {"type": "ineq", "fun": lambda A: A[6] - 0.05},
    ),
    "barra": ({"type": "ineq", "fun": lambda A: 1 - sum(A[20:-2])},),
}

bounds_dict = {
    "style": [(0, 1)] * 7 + [(None, None)],
    "barra": [(None, None)] * 20 + [(0, 1)] * 31 + [(1, 1)] + [(None, None)],
}


def cal_style_exposure(
    ret_df: pd.DataFrame, ticker: str, start_date: str, end_date: str, style_type: str
) -> pd.DataFrame:
    """
    计算风格暴露

    Parameters
    ----------
    ret_df : pd.DataFrame
        收益率的df
    ticker : str
        代码
    start_date : str
        开始时间
    end_date : str
        结束时间
    style_type : str
        风格类型

    Returns
    -------
    pd.DataFrame
        结果
    """
    cons = cons_dict[style_type]

    # 下限大于0，上限小于1，增加一个(None, None)为截距项系数
    bounds = bounds_dict[style_type]
    # 下限大于0，上限小于1，增加一个(None, None)为截距项系数
    ret_df = ret_df.copy().dropna()
    ret_df = ret_df.drop(columns=["TICKER_SYMBOL"])
    fund_ret = ret_df.loc[start_date:end_date, :]

    if fund_ret.shape[0] < 60:
        print(f"{ticker}的{start_date:}-{end_date}的{style_type}暴露长度不够!")
        return None
    y = fund_ret.iloc[:, [0]]
    x = fund_ret.iloc[:, 1:]

    try:
        result, _ = cal_exposure(
            x,
            y,
            constraints=cons,
            bounds=bounds,
        )
        result["TICKER_SYMBOL"] = ticker
        result = result.reset_index()
        result.rename(columns={"index": "TRADE_DT"}, inplace=True)
        result.columns = [col.upper() for col in result.columns]
        # print(f"{ticker}的{start_date}-{end_date}风格暴露计算完成")
        return result
    except Exception as e:
        print(e)
        return None


class FundStyleMonitor:
    def __init__(self, start_date, end_date, period="w") -> None:
        self.start_date = start_date
        self.end_date = end_date

        self.temp_start_date = dm.offset_trade_dt(self.start_date, 60)
        self.dates_list = dm.get_period_end_date()
        self.end_date_list = dm.get_period_end_date(
            start_date=start_date, end_date=end_date, period=period
        )
        self.dates_list = [
            (dm.offset_trade_dt(date, 60), date) for date in self.end_date_list
        ]
        self.code_list = (
            dm.get_own_fund_type(self.end_date)
            .query("LEVEL_1 == '权益指数' or LEVEL_1 == '主动权益'")["TICKER_SYMBOL"]
            .tolist()
        )
        self.__prepare_fund_ret()

    def __prepare_fund_ret(self) -> None:
        """
        准备基金净值数据
        """
        print("基金净值查询开始")
        fund_nav = dm.get_fund_adj_nav(
            ticker_symbol=self.code_list,
            start_date=self.temp_start_date,
            end_date=self.end_date,
        )

        fund_nav = fund_nav.pivot_table(
            index="TRADE_DT", columns="TICKER_SYMBOL", values="ADJUST_NAV"
        ).dropna(subset=["050013"])
        self.fund_ret = (
            fund_nav.pct_change()
            .unstack()
            .reset_index()
            .rename(columns={0: "ret"})
            .set_index("TRADE_DT")
        )
        print("基金净值查询完成")

    def para_cal_style(self, ret_df, style_type) -> pd.DataFrame:
        dates_list = self.dates_list
        fund_stats_list = Parallel(n_jobs=-1, backend="multiprocessing")(
            delayed(cal_style_exposure)(
                grouped_nav_df, ticker, start_date, end_date, style_type
            )
            for (start_date, end_date) in dates_list
            for ticker, grouped_nav_df in ret_df.groupby(by="TICKER_SYMBOL")
        )
        return pd.concat(fund_stats_list)

    def main(self, style_type="style") -> pd.DataFrame:
        """
        根据风格类型计算数据

        Parameters
        ----------
        style_type : str, optional
            _description_, by default "style"

        Returns
        -------
        pd.DataFrame
            结果
        """
        if style_type == "style":
            x_df = (
                dm.get_style_index(self.temp_start_date, self.end_date)
                .pct_change()
                .dropna()
            )

        elif style_type == "barra":
            x_df = dm.get_dy1d_factor_ret_cne6_sw21(self.temp_start_date, self.end_date)

        ret_df = self.fund_ret.merge(
            x_df, left_index=True, right_index=True, how="left"
        )
        # x_df.to_excel("D:/a.xlsx")
        # ret_df.to_excel("D:/b.xlsx")
        result = self.para_cal_style(ret_df, style_type)
        if style_type == "style":
            temp = result[
                [
                    "LARGE_GROWTH",
                    "LARGE_VALUE",
                    "MID_GROWTH",
                    "MID_VALUE",
                    "SMALL_GROWTH",
                    "SMALL_VALUE",
                ]
            ]
            result["STYLE"] = [val.idxmax().lower() for _, val in temp.iterrows()]
        return result

    def upsert_data(self, style_type: str = None) -> None:
        # sourcery skip: extract-duplicate-method, use-dict-items
        """
        将计算结果保存入数据库, 如果参数为None则将所有数据存入数据库

        Parameters
        ----------
        style_type : str, optional
            所需要统计的风格类型, by default None
        """

        func_dict = {
            "style": "fund_style_stock",
            "barra": "fund_exposure_cne6_sw21",
        }

        if style_type:
            df = self.main(style_type=style_type)
            DB_CONN_JJTG_DATA.upsert(df, table=func_dict[style_type])
            # DB_CONN_JJTG_DATA.upsert(df, table=func_dict[style_type])
            print(f"{func_dict[style_type]}写入完成")
            print("=*=*" * 30)
        else:
            for key in func_dict:
                df = self.main(style_type=key)
                DB_CONN_JJTG_DATA.upsert(df, table=func_dict[key])
                # DB_CONN_JJTG_DATA.upsert(df, table=func_dict[key])
                print(f"{func_dict[key]}写入完成")
                print("=*=*" * 22)


if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y%m%d")
    trade_dt_2d = dm.offset_trade_dt(today, 2)
    trade_dt_1d = dm.offset_trade_dt(today, 1)
    if dm.if_trade_dt(today):
        fund_style_monitor = FundStyleMonitor(trade_dt_2d, trade_dt_1d, period="d")
        fund_style_monitor.upsert_data()
        print("==" * 40)
        LOG_FORMAT = "TIME:%(asctime)s - Level:%(levelname)s - Mesaage:%(message)s"
        logging.basicConfig(
            filename=f"f:/BaiduNetdiskWorkspace/5-logging/fund_style_monitor{today}.log",
            level=logging.INFO,
            format=LOG_FORMAT,
        )
        logging.info(f"成功:fund_style_monitor{today}")

        # df = cal_fund_4factors_alpha(trade_dt_1d)
        # DB_CONN_JJTG_DATA.upsert(df, "fund_derivatives_monitor")
        # logging.info(f"成功:fund_derivatives_monitor{today}")
        dt_list = dm.get_trade_cal(
            start_date=trade_dt_2d,
            end_date=trade_dt_1d,
        )
        for dt in dt_list:
            query_sql = f"""
            UPDATE fund_derivatives_fund_alpha a
            INNER JOIN (
                SELECT
                    a.trade_dt AS END_DATE,
                    a.ticker_symbol,
                    c.alpha_nav_barra * (1+ ifnull( a.alpha, 0 )) AS alpha_nav_barra,
                    c.alpha_nav_style * (1+ ifnull( d.alpha, 0 )) AS alpha_nav_style
                FROM
                    fund_exposure_cne6_sw21 a
                    JOIN md_tradingdaynew b ON a.trade_dt = b.TRADE_DT
                    AND b.SECU_MARKET = 83
                    JOIN fund_derivatives_fund_alpha c ON c.END_DATE = b.PREV_TRADE_DATE
                    AND c.ticker_symbol = a.ticker_symbol
                    JOIN fund_style_stock d ON d.trade_dt = a.trade_dt
                    AND d.ticker_symbol = a.ticker_symbol
                WHERE
                    a.trade_dt = '{dt}'
                ) b ON a.END_DATE = b.END_DATE
                AND a.TICKER_SYMBOL = b.TICKER_SYMBOL
                SET a.ALPHA_NAV_BARRA = b.ALPHA_NAV_BARRA,
                a.alpha_nav_style = b.alpha_nav_style;
            """
            DB_CONN_JJTG_DATA.exec_non_query(query_sql)

        df = cal_fund_inner_alpha_performance(
            start_date=trade_dt_2d, end_date=trade_dt_1d
        )
        DB_CONN_JJTG_DATA.upsert(df, table="fund_derivatives_fund_alpha_performance")
        date = dm.get_now()
        mail_sender = MailSender()
        mail_sender.message_config_content(
            from_name="进化中的ChenGPT_0.1",
            subject=f"【数据库更新】风格监控更新完成{date}",
        )
        mail_sender.send_mail(receivers=["569253615@qq.com"])
        cal_fund_inner_alpha_model(trade_dt_1d)
