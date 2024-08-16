import pandas as pd
from joblib import Parallel, delayed

from quant_utils.performance import Performance


def cal_performance(
    nav_df: pd.DataFrame,
    ticker: str = None,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    根据净值序列计算绩效分析, 净值Series的index为日期, 如果存在start_date与end_date,
    则对时间序列进行筛选, 否则为全量数据。

    Parameters
    ----------

    nav_df : pd.DataFrame
        基金净值序列, index为日期

    ticker : str, optional
        基金代码或名称, by default None

    start_date : str, optional
        开始日期, by default None

    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    pd.DataFrame
        基金绩效分析df
    """
    temp_nav_df = nav_df.copy()

    temp_nav_series = temp_nav_df["ADJUST_NAV"]
    if "BENCHMARK" in temp_nav_df.columns:
        temp_benchmark = temp_nav_df["BENCHMARK"]
    else:
        temp_benchmark = pd.Series()

    try:
        if start_date:
            temp_nav_series = temp_nav_series[temp_nav_series.index >= start_date]
            if not temp_benchmark.empty:
                temp_benchmark = temp_benchmark[temp_benchmark.index >= start_date]
        if end_date:
            temp_nav_series = temp_nav_series[temp_nav_series.index <= end_date]
            if not temp_benchmark.empty:
                temp_benchmark = temp_benchmark[temp_benchmark.index <= end_date]
        try:
            performance_result = Performance(temp_nav_series, temp_benchmark).stats()
            if ticker:
                performance_result.columns = [ticker]
            return performance_result.T

        except Exception as e:
            # print(temp_nav_series)
            # print(f"{ticker}的{start_date}-{end_date}")
            print(e)
            return None
    except Exception as e:
        # print(temp_nav_df)
        print(e)
        return None


def parallel_cal_periods_performance(
    nav_df: pd.DataFrame, date_list: list[tuple]
) -> pd.DataFrame:
    """
    根据不同起始时间,与净值序列计算不同阶段的表现

    Parameters
    ----------
    nav_df :  pd.DataFrame
        基金净值的DataFrame, columns: TICKER_SYMBOL, ADJUST_NAV;
        index为日期

    date_list : list[tuple]
        时间tuple的list,需要时间需要为start_date与end_date之间
        tuple中第一个元素为区间开始时间,
        第二个元素为区间结束时间

    Returns
    -------
    pd.DataFrame
        绩效表现的DataFrame
    """

    # 计算同类基金的净值表现信息
    fund_stats_list = Parallel(n_jobs=-1, backend="multiprocessing")(
        delayed(cal_performance)(grouped_nav_df, ticker, start_date, end_date)
        for (start_date, end_date) in date_list
        for ticker, grouped_nav_df in nav_df.groupby(by="TICKER_SYMBOL")
    )
    if all(i is None for i in fund_stats_list):
        print("结果都是None")
        print("=*" * 30)
    else:
        return pd.concat(fund_stats_list)


def parallel_cal_func(
    func, nav_df: pd.DataFrame, date_list: list[tuple], n_jobs: int = -1
):
    """
    平行

    Parameters
    ----------
    func : function
        _description_
    nav_df : pd.DataFrame
        _description_
    date_list : list[tuple]
        _description_
    n_jobs : int, optional
        _description_, by default -1

    Returns
    -------
    _type_
        _description_
    """
    fund_stats_list = Parallel(n_jobs=n_jobs, backend="multiprocessing")(
        delayed(func)(grouped_nav_df, ticker, start_date, end_date)
        for (start_date, end_date) in date_list
        for ticker, grouped_nav_df in nav_df.groupby(by="TICKER_SYMBOL")
    )
    return pd.concat(fund_stats_list)
