import datetime

import feather
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm
import polars as pl
from quant_pl.performance_pl import PerformancePL
from dateutil.parser import parse
import quant_utils.data_moudle as dm
from fund_db.fund_db_updates import update_fund_performance_rank
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_JY_LOCAL
from quant_utils.performance import Performance
from quant_utils.utils import yield_split_list

INIT_DATE = "20210903"


def get_fund_nav_by_pl(
    start_date: str, end_date: str, parquet_path: str = "F:/data_parquet/fund_nav/"
):
    start_date = parse(start_date)
    end_date = parse(end_date)
    return (
        pl.scan_parquet(f"{parquet_path}*.parquet")
        .select(
            [
                pl.col("END_DATE").cast(pl.Datetime),
                pl.col("TICKER_SYMBOL").cast(pl.String),
                pl.col("ADJ_NAV").cast(pl.Float64).alias("NAV"),
            ]
        )
        .filter((pl.col("END_DATE") >= start_date) & (pl.col("END_DATE") <= end_date))
        .sort(
            by=["END_DATE", "TICKER_SYMBOL"],
        )
    ).collect()


def parallel_cal_performance(
    ticker: str,
    start_date: str,
    end_date: str,
    df_grouped: pd.DataFrame,
) -> pd.DataFrame:
    """
    平行计算净值的表现

    Parameters
    ----------
    ticker : str
        代码
    start_date : str
        开始日期
    end_date : str
        结束日期
    df_grouped : pd.DataFrame
        分组后的净值数据,index为日期序列,
        columns至少包含NAV,BENCHMARK_NAV或有

    Returns
    -------
    pd.DataFrame
        计算结果
    """
    df_temp = df_grouped["NAV"]
    if "BENCHMARK_NAV" in df_grouped.columns:
        benchmark_temp = df_grouped["BENCHMARK_NAV"]
    else:
        benchmark_temp = pd.Series()
    result_list = []

    try:
        fund_alpha_nav = df_temp[start_date:end_date].dropna()
        if not benchmark_temp.empty:
            benchmark_nav = benchmark_temp[start_date:end_date].dropna()
        else:
            benchmark_nav = pd.Series()
        # 数据是否以开始时间和结束时间结束
        if_condition = (
            fund_alpha_nav.empty
            or fund_alpha_nav.index[0] != start_date
            or fund_alpha_nav.index[-1] != end_date
        )
        if not if_condition:
            perf = (
                Performance(nav_series=fund_alpha_nav, benchmark_series=benchmark_nav)
                .stats(if_annual=0)
                .T
            )
            result_list.append(perf)
    except Exception as e:
        pass

    if result_list:
        tmp_result = pd.concat(result_list).set_index(
            ["起始日期", "结束日期", "最大回撤日"]
        )
        tmp_result = (
            tmp_result.where(tmp_result <= 10**8, np.inf)
            .where(tmp_result > -(10**8), -np.inf)
            .reset_index()
        )
        tmp_result["TICKER_SYMBOL"] = ticker
        return tmp_result
    # else:
    #     return None


def get_portfolio_constant_date(
    end_date: str = None, start_date: str = None
) -> pd.DataFrame:
    """
    获取组合的固定日期,包括成立日期与对客日期

    Parameters
    ----------
    end_date : str, optional
        结束日期, by default None
    start_date : str, optional
        开始日期, by default None

    Returns
    -------
    pd.DataFrame
        columns=[START_DATE, END_DATE, DATE_NAME, PORTFOLIO_NAME]
    """
    if start_date is None:
        start_date = end_date
    query_sql = f"""
    SELECT DISTINCT
        DATE_FORMAT( LISTED_DATE, "%Y%m%d" ) AS START_DATE,
        "成立日" AS DATE_NAME,
        PORTFOLIO_NAME
    FROM
        `portfolio_info` 
    WHERE
        1 = 1 
        AND IFNULL( DELISTED_DATE, "20991231" ) >= '{end_date}'
        and LISTED_DATE <= '{start_date}' UNION
    SELECT DISTINCT
        DATE_FORMAT( TO_CLIENT_DATE, "%Y%m%d" ) AS START_DATE,
        "对客日" AS DATE_NAME,
        PORTFOLIO_NAME
    FROM
        `portfolio_info` 
    WHERE
        1 = 1 
        AND IFNULL( DELISTED_DATE, "20991231" ) >= '{end_date}'
        and LISTED_DATE <= '{start_date}' 
    HAVING
        START_DATE IS NOT NULL 
        AND START_DATE <= '{end_date}'
    ORDER BY
        START_DATE
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def __helper_func(constant_dates_df, date):
    dates_list = []
    temp_df = constant_dates_df.copy()
    temp_df["END_DATE"] = date
    temp_df = temp_df.query("END_DATE > START_DATE")
    # print(temp_df)
    if temp_df.empty:
        return None

    dates_list.append(temp_df)
    ytd = dm.get_last_peroid_end_date(end_date=date, period="y")
    mtd = dm.get_last_peroid_end_date(end_date=date, period="m")
    qtd = dm.get_last_peroid_end_date(end_date=date, period="q")
    # 常用日期
    period_end_date_dict = dm.get_recent_period_end_date_dict(end_date=date)
    period_end_date_dict.update({"YTD": (ytd, date)})
    period_end_date_dict.update({"MTD": (mtd, date)})
    period_end_date_dict.update({"QTD": (qtd, date)})
    # 特别日期
    if date > "20240329":
        period_end_date_dict.update({"TGDS_1": ("20240329", date)})
    if date > "20240930":
        period_end_date_dict.update({"TGDS_2": ("20240930", date)})
    period_end_date_df = pd.DataFrame(period_end_date_dict).T
    period_end_date_df.columns = ["START_DATE", "END_DATE"]
    period_end_date_df = period_end_date_df.reset_index()
    period_end_date_df.rename(columns={"index": "DATE_NAME"}, inplace=True)
    period_end_date_df["PORTFOLIO_NAME"] = "ALL"
    dates_list.append(period_end_date_df)
    return pd.concat(dates_list)


def cal_needed_dates_df(end_date: str = None, start_date: str = None) -> pd.DataFrame:
    """
    获取需要计算的日期DataFrame

    Parameters
    ----------
    end_date : str, optional
        结束日期, by default None
    start_date : str, optional
        开始日期, by default None

    Returns
    -------
    pd.DataFrame
        columns=[START_DATE, END_DATE],
        index格式为[成立日_组合名称, 对客日_组合名称, 近X日...]
    """
    if start_date is None:
        start_date = end_date

    trade_dts = dm.get_trade_cal(start_date, end_date)

    constant_dates_df = get_portfolio_constant_date(
        start_date=start_date, end_date=end_date
    )

    # print(constant_dates_df)
    dates_list = Parallel(n_jobs=-1, backend="multiprocessing")(
        delayed(__helper_func)(constant_dates_df, date) for date in tqdm(trade_dts)
    )
    dates_df = pd.concat(dates_list)

    DB_CONN_JJTG_DATA.upsert(dates_df, table="portfolio_dates")


def get_needed_dates_df(end_date: str = None, start_date: str = None) -> pd.DataFrame:
    query_sql = f"""
    SELECT
        date_format(START_DATE,'%Y%m%d') as START_DATE,
        date_format(END_DATE,'%Y%m%d') as END_DATE,
        DATE_NAME
    FROM
        `portfolio_dates`
    WHERE
        1=1
        and (END_DATE BETWEEN '{start_date}' AND '{end_date}')
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


class BasePerformance:
    rename_dict = {
        "起始日期": "START_DATE",
        "结束日期": "END_DATE",
        "最大回撤日": "MAXDD_DATE",
        "累计收益率": "CUM_RETURN",
        "年化收益率": "ANNUAL_RETURN",
        "年化波动率": "ANNUAL_VOLATILITY",
        "收益波动比": "SHARP_RATIO_ANNUAL",
        "最大回撤": "MAXDD",
        "年化收益回撤比": "CALMAR_RATIO_ANNUAL",
        "最大回撤修复": "MAXDD_RECOVER",
        "波动率": "VOLATILITY",
        "累计收益波动比": "SHARP_RATIO",
    }

    def __init__(self, end_date: str, start_date: str = None) -> None:
        self.end_date = end_date
        self.start_date = start_date if start_date else self.end_date

    def calculate(self, table, data_name_list: str = None):
        time_stamp1 = datetime.datetime.now()
        dates_df = get_needed_dates_df(
            start_date=self.start_date, end_date=self.end_date
        )

        if data_name_list is not None:
            dates_df = dates_df[dates_df["DATE_NAME"].isin(data_name_list)]
        dates_df = dates_df[["START_DATE", "END_DATE"]].drop_duplicates()

        time_stamp2 = datetime.datetime.now()

        print(f"日期处理完成, 用时{time_stamp2 - time_stamp1}")
        df_nav_temp = self.get_nav()
        if isinstance(df_nav_temp, pd.DataFrame):
            df_nav_temp = pl.from_pandas(df_nav_temp).with_columns(
                pl.col("END_DATE").str.to_datetime(format="%Y%m%d")
            )
        time_stamp_nav = datetime.datetime.now()
        print(f"净值处理完成, 用时{time_stamp_nav - time_stamp2}")
        update_desc_flag = 0
        result_list = []
        for idx, (start_date, end_date) in dates_df.iterrows():
            perf = PerformancePL(df_nav_temp, start_date=start_date, end_date=end_date)
            result = perf.stats().collect().to_pandas()

            result_list.append(result)
            # counter += 1
        if all(i is None for i in result_list):
            tqdm.write("结果都是None")
            # tqdm.write("=*" * 30)
            update_desc_flag = 0
        else:
            result = pd.concat(result_list)
            time_stamp3 = datetime.datetime.now()
            print(f"计算完成, 用时{time_stamp3 - time_stamp_nav}")
            DB_CONN_JJTG_DATA.upsert(result, table=table)
            print(f"写入完成, 用时{datetime.datetime.now() - time_stamp3}")
            # update_desc_flag = 1
        time_stamp6 = datetime.datetime.now()
        print(f"总用时{time_stamp6 - time_stamp1}")
        # if update_desc_flag != 0:
        #     self.update_desc()

    def get_nav(self):
        pass

    def update_desc(self):
        pass


class FundPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT 
            TICKER_SYMBOL 
        FROM 
            fund_perf_desc 
        WHERE 
            1=1
            and NAV_END_DATE > IFNULL( FUND_PERF_END_DATE, '20000101' ) 
            AND NAV_END_DATE >= '{self.start_date}' 
        """
        ticker_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        ticker_df = pl.from_pandas(ticker_df)
        start_date = dm.offset_trade_dt(self.start_date, 1300)
        dates = dm.get_trade_cal(start_date, self.end_date)
        date_df = pd.DataFrame(dates, columns=["END_DATE"])
        date_df = pl.from_pandas(date_df).select(
            pl.col("END_DATE").str.to_datetime("%Y%m%d")
        )
        nav = (
            get_fund_nav_by_pl(start_date, self.end_date)
            .join(date_df, how="right", on=["END_DATE"])
            .join(ticker_df, how="inner", on=["TICKER_SYMBOL"])
            .with_columns(
                NAV=pl.col("NAV")
                .fill_null(strategy="forward")
                .over("TICKER_SYMBOL", order_by="END_DATE"),
            )
            .drop_nulls(subset=["NAV"])
            .sort(["TICKER_SYMBOL", "END_DATE"])
        )

        # nav["END_DATE"] = nav["END_DATE"].apply(lambda x: x.strftime("%Y%m%d"))
        # nav = nav.sort_values(by=["END_DATE", "TICKER_SYMBOL"])
        # nav_list = []
        # for _, df in nav.groupby(by="TICKER_SYMBOL"):
        #     temp_df = df.copy()
        #     temp_df = date_df.merge(temp_df, how="left", on=["END_DATE"])
        #     temp_df = temp_df.ffill().dropna()
        #     nav_list.append(temp_df)
        # nav = pd.concat(nav_list)
        # nav.rename(columns={"ADJ_NAV": "NAV"}, inplace=True)
        # print(nav)
        return nav

    def update_desc(self):
        query_sql = """
        SELECT
            TICKER_SYMBOL,
            max( END_DATE ) AS FUND_PERF_END_DATE,
            min( END_DATE ) AS FUND_PERF_START_DATE 
        FROM
            fund_performance_inner 
        GROUP BY
            TICKER_SYMBOL
        """
        df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        DB_CONN_JJTG_DATA.upsert(df, table="fund_perf_desc")


class PortfolioPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
            a.PORTFOLIO_NAME AS TICKER_SYMBOL,
            PORTFOLIO_NAV AS NAV 
        FROM
            portfolio_nav a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT >= b.LISTED_DATE
            AND a.TRADE_DT <= '{self.end_date}'
        """
        return DB_CONN_JJTG_DATA.exec_query(query_sql)


class BenchmarkPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
            a.PORTFOLIO_NAME AS TICKER_SYMBOL,
            (1 + BENCHMARK_RET_ACCUMULATED_INNER/100) AS NAV 
        FROM
            portfolio_derivatives_ret a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT >= b.LISTED_DATE
            AND a.TRADE_DT <= '{self.end_date}'
        """
        return DB_CONN_JJTG_DATA.exec_query(query_sql)


class PeerPortfolioPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            InnerCode AS TICKER_SYMBOL,
            DATE_FORMAT( EndDate, '%Y%m%d' ) AS END_DATE,
            DataValue + 1 as NAV
        FROM
            mf_portfolioperform 
        WHERE
            1 = 1 
            and (EndDate BETWEEN '{INIT_DATE}' AND '{self.end_date}')
            and StatPeriod = 999
            AND IndicatorCode = 66
        """
        df = DB_CONN_JY_LOCAL.exec_query(query_sql)
        df["NAV"] = df["NAV"].astype("float")
        return df


class PortfolioDerivatiesPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
            a.PORTFOLIO_NAME AS TICKER_SYMBOL,
            (portfolio_ret_ACCUMULATED/100 + 1) AS NAV 
        FROM
            portfolio_derivatives_ret a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT >= b.LISTED_DATE
            AND a.TRADE_DT <= '{self.end_date}'
        """
        df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        return df


def update_performance_inner(start_date, end_date):
    cal_needed_dates_df(start_date=start_date, end_date=end_date)
    fund_perf = FundPerformance(start_date=start_date, end_date=end_date)
    fund_perf.calculate("fund_performance_inner")
    port_perf = PortfolioPerformance(start_date=start_date, end_date=end_date)
    port_perf.calculate("portfolio_performance_inner")

    port_derivatives_perf = PortfolioDerivatiesPerformance(
        start_date=start_date, end_date=end_date
    )
    port_derivatives_perf.calculate("portfolio_derivatives_performance_inner")

    benchmark_perf = BenchmarkPerformance(start_date=start_date, end_date=end_date)
    benchmark_perf.calculate("benchmark_performance_inner")

    peer_perf = PeerPortfolioPerformance(start_date=start_date, end_date=end_date)
    peer_perf.calculate("peer_performance_inner")


if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y%m%d")
    start_date = dm.offset_trade_dt(LAST_TRADE_DT, 2)
    end_date = LAST_TRADE_DT
    date_list = dm.get_trade_cal(start_date, end_date)

    cal_needed_dates_df(start_date=start_date, end_date=end_date)

    update_performance_inner(start_date=start_date, end_date=end_date)
    for date in date_list:
        print(date)
        update_fund_performance_rank(date)
