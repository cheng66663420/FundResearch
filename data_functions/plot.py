# -*- coding: utf-8 -*-
from functools import reduce

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import ticker

import data_functions.fund_data as fds
import data_functions.portfolio_data as pds
import quant_utils.data_moudle as dm
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.utils import make_dirs

plt.rcParams["font.sans-serif"] = ["SimHei"]  # 用来正常显示中文标签
plt.rcParams["axes.unicode_minus"] = False  # 用来正常显示负号


def plot_nav_alpha(df: pd.DataFrame, title_name: str, file_path="D:/画图/"):
    plt_df = df.copy()
    fig, ax1 = plt.subplots(figsize=(10, 5))
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["font.family"] = "SimHei"
    plt.yticks(fontsize=14)
    (l1,) = ax1.plot(
        plt_df["TRADE_DT"],
        plt_df["PORTFOLIO_NAV"],
        color="#a31939",
        linewidth=2.5,
    )
    (l2,) = ax1.plot(
        plt_df["TRADE_DT"],
        plt_df["BENCHMARK_NAV"],
        color="#4081c1",
        linewidth=2.5,
    )
    ax2 = ax1.twinx()
    (l3,) = ax2.stackplot(
        plt_df["TRADE_DT"],
        plt_df["ALPHA"],
        color="#eaeaea",
        alpha=0.3,
    )
    ax2.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1, decimals=2))
    plt.legend(
        handles=[l1, l2, l3], labels=["组合净值", "基准净值", "超额%(右轴)"], loc=2
    )
    plt.title(title_name, loc="center", fontsize=20)
    make_dirs(file_path)
    plt.savefig(f"{file_path}{title_name}.png")


def plot_multiple_nav_alpha(
    df: pd.DataFrame, title_name_list: list, file_path="D:/画图/"
):
    df = df.copy()
    plt_len = len(title_name_list)

    plt.figure(figsize=(10, 5 * plt_len))
    for i in range(1, plt_len + 1):
        plt_df = df.query(f"PORTFOLIO_NAME == '{title_name_list[i-1]}'")
        ax1 = plt.subplot(plt_len, 1, i)
        plt.rcParams["axes.unicode_minus"] = False
        plt.rcParams["savefig.dpi"] = 300
        plt.rcParams["font.family"] = "SimHei"
        plt.yticks(fontsize=10)
        (l1,) = ax1.plot(
            plt_df["TRADE_DT"],
            plt_df["PORTFOLIO_NAV"],
            color="#a31939",
            linewidth=2.5,
        )
        (l2,) = ax1.plot(
            plt_df["TRADE_DT"],
            plt_df["BENCHMARK_NAV"],
            color="#4081c1",
            linewidth=2.5,
        )
        ax2 = ax1.twinx()
        (l3,) = ax2.stackplot(
            plt_df["TRADE_DT"],
            plt_df["ALPHA"],
            color="#eaeaea",
            alpha=0.3,
        )
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        # ax1.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=100, decimals=2))
        ax2.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1, decimals=2))
        plt.legend(
            handles=[l1, l2, l3], labels=["组合净值", "基准净值", "超额%(右轴)"], loc=2
        )
        plt.title(title_name_list[i - 1], loc="center", fontsize=14)
        for j in ax1.get_xticklabels():
            j.set_rotation(30)

    plt.tight_layout()
    make_dirs(file_path)
    plt.savefig(f"{file_path}汇总图.png")


if __name__ == "__main__":
    portfolio_info = pds.get_portfolio_info().query(
        "IF_LISTED == 1 and PORTFOLIO_TYPE != '目标盈'"
    )
    portfolio_info["LISTED_DATE"] = pd.to_datetime(portfolio_info["LISTED_DATE"])
    portfolio_info = portfolio_info[portfolio_info["LISTED_DATE"] < LAST_TRADE_DT]
    portfolio_info = portfolio_info.sort_values(by="ORDER_ID")
    portfolio_info["ORDER"] = range(1, len(portfolio_info) + 1)
    dfs_list = []
    for _, val in portfolio_info.iterrows():
        end_date = (
            dm.offset_trade_dt(LAST_TRADE_DT, 1)
            if val["INCLUDE_QDII"] == 1
            else LAST_TRADE_DT
        )

        df = pds.query_portfolio_alpha(
            portfolio_name=val["PORTFOLIO_NAME"], end_date=end_date
        )
        dfs_list.append(df)
        plot_nav_alpha(df, f'{val["PORTFOLIO_NAME"]}')
    dfs = pd.concat(dfs_list)
    plot_multiple_nav_alpha(dfs, portfolio_info["PORTFOLIO_NAME"].tolist())
    dfs.to_excel("D:/画图/净值数据.xlsx")
