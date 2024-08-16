# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 14:24:22 2022

@author: Wilcoxon
"""

import empyrical
import numpy as np
import pandas as pd
import statsmodels.api as sm
from dateutil.parser import parse

import quant_utils.data_moudle as dm

np.seterr("ignore")


class Performance:
    """
    计算绩效评价指标
    """

    def __init__(
        self,
        nav_series: pd.Series,
        benchmark_series: pd.Series = pd.Series(dtype="float64"),
        fact_return_series: pd.Series = None,
    ):
        """
        Parameters
        ----------
        nav_series : pd.Series
            净值时间序列
        benchmark_series : pd.Series, optional
            基准净值时间序列, by default pd.Series(dtype="float64")
        fact_return_series : pd.Series, optional
            因子收益时间序列, by default None
        """

        self.nav_series = nav_series
        # self.nav_series.index = pd.to_datetime(self.nav_series.index)
        self.benchmark_series = benchmark_series
        # 计算普通收益率
        self.ret_pct = self.nav_series.pct_change().dropna()
        # 计算对数收益率
        self.ln_ret = np.log(self.nav_series / self.nav_series.shift(1)).dropna()
        self.fact_return_series = fact_return_series

        # 如果存在基准收益序列
        if not self.benchmark_series.empty:
            # 基准的普通收益率
            self.benchmark_ret = self.benchmark_series.pct_change().dropna()
            # self.benchmark_ret.index = pd.to_datetime(self.benchmark_ret.index)
            # 基准的对数收益率
            self.ln_benchmark_ret = np.log(
                self.benchmark_series / self.benchmark_series.shift(1)
            ).dropna()
            # 超额收益率
            self.alpha_ret = self.ret_pct - self.benchmark_ret
            # 对数超额收益率
            self.ln_alpha = self.ln_ret - self.ln_benchmark_ret
            # 累计对数超额收益率
            self.cum_ln_alpha = self.ln_alpha.cumsum()

    def _check_benchmark(func):
        """
        装饰器,检查是否存在benchmark_series,不存在则返回空的数据，存在则运行函数
        Returns:
        """

        def wrapper(self, *args, **kwargs):
            # 判断是否存在benchmark_series，如存在执行，否则返回None
            if not self.benchmark_series.empty:
                result = func(self, *args, **kwargs)
                return result
            else:
                return None

        return wrapper

    def cum_returns_final(self):
        """
        计算累计收益率
        Returns:
            float
        """
        return np.nan_to_num(empyrical.cum_returns_final(self.ret_pct))

    @_check_benchmark
    def benchmark_cum_returns_finals(self) -> float:
        """
        计算benchmark的累计收益率

        Returns
        -------
        float
            基准的累计收益率
        """
        return np.nan_to_num(empyrical.cum_returns_final(self.benchmark_ret))

    @_check_benchmark
    def benchmark_annual_return(self) -> float:
        """
        基准的年化收益率

        Returns
        -------
        float
            基准的年化收益率
        """
        start_date = self.benchmark_ret.index[0]
        end_date = self.benchmark_ret.index[-1]

        if isinstance(start_date, str):
            dates = (parse(end_date) - parse(start_date)).days + 1

        else:
            dates = (end_date - start_date).days + 1
        cum_ret = self.benchmark_cum_returns_finals()

        return (cum_ret + 1) ** (365 / dates) - 1

    @_check_benchmark
    def IR(self):
        """
        计算信息比率
        Returns:
            float
        """
        start_date = self.benchmark_ret.index[0]
        end_date = self.benchmark_ret.index[-1]

        if isinstance(start_date, str):
            dates = (parse(end_date) - parse(start_date)).days + 1

        else:
            dates = (end_date - start_date).days + 1

        cum_alpha = np.exp(np.sum(self.ln_alpha)) - 1
        cum_alpha_annual = (cum_alpha + 1) ** (365 / dates) - 1
        return np.nan_to_num(
            cum_alpha_annual / empyrical.annual_volatility(self.alpha_ret)
        )

    def annual_return(self):
        """
        计算年化收益率
        Returns:
            float
        """

        start_date = self.ret_pct.index[0]
        end_date = self.ret_pct.index[-1]

        if isinstance(start_date, str):
            dates = (parse(end_date) - parse(start_date)).days + 1

        else:
            dates = (end_date - start_date).days + 1
        cum_ret = self.cum_returns_final()

        return (cum_ret + 1) ** (365 / dates) - 1

    def annual_volatility(self):
        """
        计算年化波动率
        Returns:
            float
        """
        return np.nan_to_num(empyrical.annual_volatility(self.ret_pct))

    def volatility(self):
        """
        计算波动率(非年化)
        Returns:
            float
        """
        return self.ret_pct.std()

    @_check_benchmark
    def benchmark_annual_volatility(self) -> float:
        """
        计算基准的年化波动率

        Returns
        -------
        float
            基准的年化波动率
        """
        return np.nan_to_num(empyrical.annual_volatility(self.benchmark_ret))

    def sharpe_ratio(self, risk_free=0.0):
        """
        计算夏普比率
        Returns:
            float
        """
        return (self.annual_return() - risk_free) / self.annual_volatility()

    @_check_benchmark
    def alpha(self):
        """
        计算超额收益率
        Returns:
            float
        """
        return np.nan_to_num(
            self.cum_returns_final() - self.benchmark_cum_returns_finals()
        )

    @_check_benchmark
    def annual_alpha(self):
        """
        计算超额收益率
        Returns:
            float
        """
        start_date = self.benchmark_ret.index[0]
        end_date = self.benchmark_ret.index[-1]

        if isinstance(start_date, str):
            dates = (parse(end_date) - parse(start_date)).days + 1

        else:
            dates = (end_date - start_date).days + 1
        return np.nan_to_num((1 + self.alpha()) ** (365 / dates) - 1)

    @_check_benchmark
    def down_capture(self):
        """
        计算下行捕捉比率
        Returns:
            float
        """
        return np.nan_to_num(empyrical.down_capture(self.ret_pct, self.benchmark_ret))

    @_check_benchmark
    def up_capture(self):
        """
        计算上行比率
        Returns:
            float
        """
        return np.nan_to_num(empyrical.up_capture(self.ret_pct, self.benchmark_ret))

    @_check_benchmark
    def up_down_capture(self):
        """
        计算上下捕捉比率
        Returns:
            float
        """
        return np.nan_to_num(
            empyrical.up_down_capture(self.ret_pct, self.benchmark_ret)
        )

    def stability(self):
        """
        计算累计收益率相对于时间的R方
        Returns:
            float
        """
        return np.nan_to_num(empyrical.stability_of_timeseries(self.ret_pct))

    @_check_benchmark
    def stability_of_alpha_timeseries(self):
        """
        计算累计超额收益相对于时间的R方
        Returns:
            float
        """
        t = list(range(1, len(self.cum_ln_alpha) + 1))
        model = sm.OLS(self.cum_ln_alpha, t).fit()
        return model.rsquared

    def prof_rate(self):
        """
        计算盈利占比
        Returns:
            float
        """
        return len(self.ret_pct[self.ret_pct > 0]) / len(self.ret_pct)

    @_check_benchmark
    def alpha_prof_rate(self):
        """
        计算超额收益盈利占比
        Returns:
            float
        """
        return len(self.ln_alpha[self.ln_alpha > 0]) / len(self.ln_alpha)

    def max_drawdown(self, nav_series=pd.Series(dtype="float64")):
        """
        最大回撤
        Returns:
            float
        """
        if nav_series.empty:
            nav_series = self.nav_series

        # 计算回撤
        drawdowns = nav_series / nav_series.cummax() - 1
        # 最大回撤值
        max_drawdown = drawdowns.min()
        return -max_drawdown

    @_check_benchmark
    def benchmark_max_drawdown(self, nav_series=pd.Series(dtype="float64")):
        """
        基准最大回撤
        Returns:
            float
        """
        if nav_series.empty:
            benchmark_series = self.benchmark_series
        # 计算回撤
        drawdowns = benchmark_series / benchmark_series.cummax() - 1
        # 最大回撤值
        max_drawdown = drawdowns.min()
        return -max_drawdown

    def max_drawdown_recover(self, nav_series=pd.Series(dtype="float64")):
        """
        计算最大回撤修复期
        Returns:
            int天数
        """
        if nav_series.empty:
            nav_series = self.nav_series

        # 计算回撤
        drawdowns = nav_series / nav_series.cummax() - 1
        # 最大回撤值
        max_drawdown = drawdowns.min()
        # 最大回撤发生日
        max_drawdown_date = drawdowns[drawdowns == max_drawdown].index[0]

        drawdown_after_max = drawdowns[drawdowns.index > max_drawdown_date]
        drawdown_after_max = drawdown_after_max[drawdown_after_max == 0]
        if drawdown_after_max.empty:
            max_drawdown_duration = 99999
            if isinstance(max_drawdown_date, str):
                max_drawdown_date = parse(max_drawdown_date)
        else:
            max_drawdown_recover_date = drawdown_after_max.index[0]
            if isinstance(max_drawdown_recover_date, str):
                max_drawdown_recover_date = parse(max_drawdown_recover_date)
            if isinstance(max_drawdown_date, str):
                max_drawdown_date = parse(max_drawdown_date)
            max_drawdown_duration = (max_drawdown_recover_date - max_drawdown_date).days

        max_drawdown_date = max_drawdown_date.strftime("%Y-%m-%d")
        return max_drawdown_date, max_drawdown_duration

    @_check_benchmark
    def alpha_max_drawdown(self):
        """
        计算累计超额收益最大回撤
        Returns:
            float
        """
        return self.max_drawdown((self.alpha_ret + 1).cumprod())

    @_check_benchmark
    def alpha_max_drawdown_recover(self):
        """
        计算累计超额收益率最大回撤修复期
        Returns:
            int 天数
        """
        return self.max_drawdown_recover((self.alpha_ret + 1).cumprod())

    def calmar_ratio(self):
        """
        计算卡尔玛指数
        Returns:
            float
        """
        return np.nan_to_num(self.annual_return() / np.abs(self.max_drawdown()))

    def value_at_risk(self, cutoff=0.05):
        """
        计算在险价值
        Args:
            cutoff: 默认5%
        Returns:
            float
        """
        return np.nan_to_num(empyrical.value_at_risk(self.ret_pct, cutoff))

    @_check_benchmark
    def T_M_model(self, risk_free=0.03):
        """
        T-M模型
        Args:
            risk_free: 无风险利率

        Returns:
            alpha_anual: float 年化aplha
            timing: 择时beta
        """
        # T-M因变量为组合收益率-无风险利率
        ex_rp = self.ret_pct - risk_free / 252
        # 基准收益率-无风险利率
        X = pd.DataFrame()
        X["ex_rm"] = self.benchmark_ret - risk_free / 252
        # (基准收益率-无风险利率)的平方
        X["ex_rm_sqr"] = X["ex_rm"] ** 2
        # 模型自变量矩阵
        x_TM = sm.add_constant(X)
        # 构建模型
        if len(ex_rp) != len(x_TM):
            return None, None
        TM = sm.OLS(ex_rp, x_TM).fit()
        # # 打印summary
        # print(TM.summary())
        # 获取系数，alpha需要进行年化
        alpha_anual = (1 + TM.params[0]) ** 252 - 1
        selection = TM.params[1]
        timing = TM.params[2]
        return alpha_anual, timing

    @_check_benchmark
    def C_L_model(self, risk_free=0.03):
        """
        C-L模型
        Args:
            risk_free: 无风险利率

        Returns:
            alpha_anual: float 年化aplha
            timing: 择时能力
        """
        # T-M因变量为组合收益率-无风险利率
        ex_rp = self.ret_pct - risk_free / 252
        # 基准收益率-无风险利率
        X = pd.DataFrame()
        X["ex_rm"] = self.benchmark_ret - risk_free / 252
        X["Ex_Rm+"], X["Ex_Rm-"] = X["ex_rm"].copy(), X["ex_rm"].copy()
        for i in X.index:
            if X["ex_rm"][i] >= 0:
                X["Ex_Rm+"][i] = X["ex_rm"][i]
                X["Ex_Rm-"][i] = 0
            else:
                X["Ex_Rm-"][i] = X["ex_rm"][i]
                X["Ex_Rm+"][i] = 0
        # 模型自变量矩阵

        # # C-L模型输入变量矩阵
        X_CL = sm.add_constant(X[["Ex_Rm-", "Ex_Rm+"]])
        # C-L拟合模型
        if len(ex_rp) != len(X_CL):
            return None, None

        CL = sm.OLS(ex_rp, X_CL).fit()
        # print(CL.summary())
        # 获取系数，alpha需要进行年化
        alpha_anual = (1 + CL.params[0]) ** 252 - 1
        timing = CL.params[2] - CL.params[1]

        return alpha_anual, timing

    # def alpha_barra_model(self, risk_free=0.03):
    #     """
    #     获取barra模型的alpha(年化)
    #     Returns:

    #     """
    #     # 开始时间，结束时间
    #     start_date, end_date = self.ret_pct.index[0].strftime(
    #         "%Y-%m-%d"
    #     ), self.ret_pct.index[-1].strftime("%Y-%m-%d")
    #     # 如果数据为空，则根据收益数据提取数据
    #     if self.fact_return_series is None or self.fact_return_series.empty:

    #         # 提取因子数据
    #         factor_ret = get_dy1d_factor_ret(start_date, end_date)
    #         # 获取国家因子并，将国家收益从因子收益中删除
    #         coutry_ret = factor_ret["COUNTRY"].copy()
    #         factor_ret = factor_ret.drop(columns=["COUNTRY"])
    #         # 如果factor_ret为空
    #         if factor_ret.empty or (factor_ret.shape[0] != len(self.ret_pct)):
    #             print()
    #             return None

    #         # 如果factor_ret的index最后不为结束日期
    #         if factor_ret.index[-1].strftime("%Y-%m-%d") != end_date:
    #             return None

    #     else:
    #         factor_ret = self.fact_return_series.copy()

    #         try:
    #             start_date1 = pd.to_datetime(start_date)
    #             end_date1 = pd.to_datetime(end_date)
    #             factor_ret = factor_ret.loc[start_date1:end_date1, :]
    #             coutry_ret = factor_ret["COUNTRY"].copy()
    #             factor_ret = factor_ret.drop(columns=["COUNTRY"])

    #         except Exception as e:
    #             print(e)
    #             return None

    #     if len(self.ret_pct) != len(factor_ret):
    #         return None
    #     # barra模型输入变量矩阵
    #     try:
    #         barra = sm.add_constant(factor_ret)
    #         model = sm.OLS(self.ret_pct - coutry_ret, barra).fit()
    #         # print(model.summary())
    #         # 返回年化数据
    #         alpha = (1 + model.params[0]) ** 252 - 1

    #     except Exception as e:
    #         print(e)
    #         return None

    #     else:
    #         return alpha

    def stats(self, if_annual=1):  # sourcery skip: extract-method
        """
        绩效评估结果
        Returns:
            pandas.Dataframe
        """

        # 创建有序字典
        perf_dict = {
            "起始日期": self.nav_series.index[0],
            "结束日期": self.nav_series.index[-1],
            "累计收益率": self.cum_returns_final(),
        }

        perf_dict["年化收益率"] = self.annual_return()
        perf_dict["年化波动率"] = self.annual_volatility()
        perf_dict["收益波动比"] = np.nan_to_num(
            perf_dict["年化收益率"] / perf_dict["年化波动率"]
        )
        perf_dict["最大回撤"] = self.max_drawdown()

        if not self.benchmark_series.empty:
            perf_dict["基准累计收益率"] = self.benchmark_cum_returns_finals()
            perf_dict["基准年化收益率"] = self.benchmark_annual_return()
            perf_dict["基准年化波动率"] = self.benchmark_annual_volatility()
            perf_dict["基准收益波动比"] = (
                perf_dict["基准年化收益率"] / perf_dict["基准年化波动率"]
            )
            perf_dict["基准最大回撤"] = self.benchmark_max_drawdown()
            perf_dict["超额收益"] = self.alpha()
            perf_dict["IR"] = self.IR()

        perf_dict["年化收益回撤比"] = (
            perf_dict["年化收益率"] / perf_dict["最大回撤"]
            if perf_dict["最大回撤"] != 0
            else np.inf
        )

        perf_dict["最大回撤日"], perf_dict["最大回撤修复"] = self.max_drawdown_recover()
        if if_annual != 1:
            perf_dict["波动率"] = self.volatility()
            perf_dict["累计收益波动比"] = perf_dict["累计收益率"] / perf_dict["波动率"]

        return pd.DataFrame.from_dict(perf_dict, orient="index")

    def rolling_ret_stats(self):
        """滚动收益率统计

        Returns
        -------
        DataFrame
            _description_
        """
        perf_dict = {}
        dict_name_map = {
            7: "7天",
            20: "1个月",
            40: "2个月",
            60: "3个月",
            120: "6个月",
            180: "9个月",
            240: "1年",
            480: "2年",
            720: "3年",
            960: "4年",
            1200: "5年",
        }

        for key, val in dict_name_map.items():
            ret_period = self.nav_series / self.nav_series.shift(key) - 1

            perf_dict[val] = {"收益率25分位数": (ret_period).quantile(0.25)}
            perf_dict[val]["收益率中位数"] = (ret_period).median()
            perf_dict[val]["收益率75分位数"] = (ret_period).quantile(0.75)
            perf_dict[val]["胜率"] = (
                ret_period[ret_period > 0].count() / ret_period.count()
            )

        return pd.DataFrame.from_dict(perf_dict).applymap(lambda x: format(x, ".2%"))


def analysis_ic(ic_list):
    """
    进行IC分析
    Args:
        ic_list: ic序列

    Returns:
        DataFrame: IC结果
    """
    # IC均值与IR
    # 转换成array
    ic_list = np.array(ic_list)
    # IC序列个数
    ic_num = len(ic_list)
    # IC均值
    ic_mean = np.mean(ic_list)
    # IC标准差
    ic_std = np.std(ic_list)
    # IR
    ir = ic_mean / ic_std
    # t值
    t_value = ic_mean / (np.std(ic_list) / np.sqrt(len(ic_list)))
    # ic正比例
    ic_positive = len(ic_list[ic_list > 0]) / len(ic_list)
    # ic负比例
    ic_negative = len(ic_list[ic_list < 0]) / len(ic_list)
    return pd.DataFrame(
        [ic_num, ic_mean, abs(ic_mean), ic_std, ir, t_value, ic_positive, ic_negative],
        index=[
            "IC序列个数",
            "IC均值",
            "IC绝对值",
            "IC标准差",
            "IR",
            "t_value",
            "IC正比例",
            "IC负比例",
        ],
    ).T


def periods_performance(
    nav_series: pd.Series, benckmark_series: pd.Series = pd.Series(dtype="float64")
):

    nav_series = nav_series.copy()
    benckmark_series = benckmark_series.copy()
    nav_series.index = [parse(str(s)).strftime("%Y%m%d") for s in nav_series.index]
    benckmark_series.index = [
        parse(str(s)).strftime("%Y%m%d") for s in benckmark_series.index
    ]
    start_date = nav_series.index[0]
    end_date = nav_series.index[-1]
    # 计算组合表现绩效
    portfolio_performance = Performance(nav_series, benckmark_series).stats().T
    portfolio_performance["PERIOD"] = "ALL"
    portfolio_performance["PERIOD_NAME"] = "ALL"
    perf_list = [portfolio_performance]
    peroid_df = dm.get_period_end(start_date, end_date)
    peroid_df.rename(
        columns={"START_DATE": "起始日期", "END_DATE": "结束日期"}, inplace=True
    )
    for _, v in peroid_df.iterrows():
        temp_nav = nav_series[
            (nav_series.index >= v["起始日期"]) & (nav_series.index <= v["结束日期"])
        ]
        temp_benchmark = benckmark_series[
            (benckmark_series.index >= v["起始日期"])
            & (benckmark_series.index <= v["结束日期"])
        ]
        if temp_nav.shape[0] > 1:
            temp = Performance(temp_nav, temp_benchmark).stats().T
            temp["PERIOD"] = v["PERIOD"]
            temp["PERIOD_NAME"] = v["PERIOD_NAME"]
            perf_list.append(temp)
    return pd.concat(perf_list)


if __name__ == "__main__":
    from quant_utils.data_moudle import get_fund_adj_nav

    nav = get_fund_adj_nav("018448", "20230505", "20231107")
    # print(nav)
    # ret_df = nav[['end_date', '进取全明星——财富增长', '业绩比较基准']]
    # ret_df = ret_df.query("endDate> '20161231' and endDate< '20210301'")
    nav.set_index("TRADE_DT", inplace=True)
    perf = Performance(nav["ADJUST_NAV"])
    print(perf.stats(if_annual=0))
    # print(perf.stats().T)
