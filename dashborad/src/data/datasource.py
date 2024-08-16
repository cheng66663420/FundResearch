from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
from src.constant import DATE_FORMAT

from quant_utils.performance import Performance


@dataclass
class PortRetDataSource:
    # 收益率数据的一个
    data: pd.DataFrame

    def __post_init__(self):
        df = self.data.copy()
        df["TRADE_DT"] = pd.to_datetime(df["TRADE_DT"]).dt.strftime(DATE_FORMAT)
        df["ALPHA"] = (
            df["PORTFOLIO_RET_ACCUMULATED"] - df["BENCHMARK_RET_ACCUMULATED_INNER"]
        )
        df["NAV"] = df["PORTFOLIO_RET_ACCUMULATED"] / 100 + 1
        df["MAX_NAV"] = df["NAV"].cummax()

        df["MAX_DRAWDOWN"] = (df["NAV"] / df["MAX_NAV"] - 1) * 100
        self.data = df

    def filter_by_date(self, start_date: str, end_date: str) -> PortRetDataSource:
        # 根据日期筛选数据
        df = self.data.query("TRADE_DT >= @start_date and TRADE_DT <= @end_date")
        return PortRetDataSource(data=df)

    def normlized(self) -> PortRetDataSource:
        # 归一化
        df = self.data.copy()
        df.index = range(0, df.shape[0])
        df["PORTFOLIO_RET_ACCUMULATED"] = df["PORTFOLIO_RET_ACCUMULATED"] / 100 + 1
        df["PORTFOLIO_RET_ACCUMULATED"] = (
            df["PORTFOLIO_RET_ACCUMULATED"] / df["PORTFOLIO_RET_ACCUMULATED"][0] - 1
        ) * 100
        df["BENCHMARK_RET_ACCUMULATED_INNER"] = (
            df["BENCHMARK_RET_ACCUMULATED_INNER"] / 100 + 1
        )
        df["BENCHMARK_RET_ACCUMULATED_INNER"] = (
            df["BENCHMARK_RET_ACCUMULATED_INNER"]
            / df["BENCHMARK_RET_ACCUMULATED_INNER"][0]
            - 1
        ) * 100
        df["ALPHA"] = (
            df["PORTFOLIO_RET_ACCUMULATED"] - df["BENCHMARK_RET_ACCUMULATED_INNER"]
        )
        df["NAV"] = df["PORTFOLIO_RET_ACCUMULATED"] / 100 + 1
        df["MAX_NAV"] = df["NAV"].cummax()
        df["MAX_DRAWDOWN"] = (df["NAV"] / df["MAX_NAV"] - 1) * 100
        return PortRetDataSource(data=df)

    def get_performance(self) -> pd.DataFrame:
        #  获取绩效指标
        dff = self.data.copy()
        dff.set_index("TRADE_DT", inplace=True)
        portfolio_perf = Performance(dff["NAV"]).stats().T

        # portfolio_perf.columns = ["指标", "数值"]
        return portfolio_perf

    @property
    def name(self) -> str:
        return self.data["PORTFOLIO_NAME"].iloc[0]

    @property
    def trade_dates(self) -> list[str]:
        return self.data["TRADE_DT"].tolist()

    def to_dict(self, orient) -> dict:
        return self.data.to_dict(orient=orient)
