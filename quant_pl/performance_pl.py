import datetime
from dataclasses import dataclass

import numpy as np
import polars as pl
from dateutil.parser import parse


def _parse_df(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    解析净值DataFrame，将END_DATE转换为datetime，TICKER_SYMBOL转换为str，NAV转换为float

    Parameters
    ----------
    df : pl.LazyFrame
        LazyFrame, 包含列: TICKER_SYMBOL, END_DATE, NAV

    Returns
    -------
    pl.DataFrame
        解析后的DataFrame, 包含列: END_DATE, TICKER_SYMBOL, NAV

    Raises
    ------
    ValueError
        _description_
    ValueError
        _description_
    """
    if not isinstance(df, pl.LazyFrame):
        raise ValueError("df must be a polars LazyFrame")
    try:
        return df.select(
            [
                pl.col("END_DATE").cast(pl.Datetime),
                pl.col("TICKER_SYMBOL").cast(pl.Utf8),
                pl.col("NAV").cast(pl.Float64),
            ]
        )
    except Exception as exc:
        print(exc)
        raise ValueError(
            "df must contain columns: TICKER_SYMBOL, END_DATE, NAV"
        ) from exc


def _filter_df(
    df: pl.LazyFrame, start_date: datetime.datetime, end_date: datetime.datetime
) -> pl.LazyFrame:
    """
    筛选出指定日期范围内的基金，只保留开始日期和结束日期都在指定范围内的基金

    Parameters
    ----------
    df : pl.LazyFrame
        净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV
    start_date : datetime.datetime
        开始日期
    end_date : datetime.datetime
        结束日期

    Returns
    -------
    pl.LazyFrame
        筛选后的净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV
    """
    return (
        df.filter(pl.col("END_DATE").is_between(start_date, end_date))
        # 计算每个基金的开始日期和结束日期
        .with_columns(
            START_DATE=pl.col("END_DATE").min().over("TICKER_SYMBOL"),
            MAX_DATE=pl.col("END_DATE").max().over("TICKER_SYMBOL"),
        )
        # 筛选出开始日期和结束日期都在指定范围内的基金
        .filter((pl.col("START_DATE") == start_date) & (pl.col("MAX_DATE") == end_date))
        .select(pl.all().exclude("MAX_DATE"))
    )


def _cal_operation_days(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算每个基金的操作天数

    Parameters
    ----------
    df : pl.LazyFrame
        净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV

    Returns
    -------
    pl.DataFrame
        净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV, OPERATION_DAYS
    """
    return (
        df
        # 计算每个基金的下一个交易日的日期
        .with_columns(
            OPERATION_DATE=pl.col("END_DATE")
            .shift(-1)
            .over("TICKER_SYMBOL", order_by="END_DATE")
        )
        # 取最小值
        .with_columns(
            OPERATION_DATE=pl.col("OPERATION_DATE")
            .min()
            .over("TICKER_SYMBOL", order_by="END_DATE")
        )
        # 计算操作天数
        .with_columns(
            OPERATION_DAYS=(
                (pl.col("END_DATE") - pl.col("OPERATION_DATE")).dt.total_days() + 1
            )
        )
        # 处理操作天数为负数的情况
        .with_columns(
            OPERATION_DAYS=pl.when(pl.col("OPERATION_DAYS") < 0)
            .then(0)
            .otherwise(pl.col("OPERATION_DAYS"))
        )
    )


@dataclass
class PerformanceHelper:
    df: pl.DataFrame | pl.LazyFrame

    def __post_init__(self):
        if isinstance(self.df, pl.DataFrame):
            self.df = self.df.lazy()
        self.df = self.df.sort(
            by=["TICKER_SYMBOL", "END_DATE"],
        )

    def data_name(self) -> pl.LazyFrame:
        return self.df.group_by("TICKER_SYMBOL").agg(
            START_DATE=pl.col("END_DATE").min(),
            END_DATE=pl.col("END_DATE").max(),
            OPERATION_DAYS=pl.col("OPERATION_DAYS").max(),
        )

    def daily_return(self) -> pl.LazyFrame:
        """
        计算每个基金的日收益率*100
        """
        return self.df.with_columns(
            DAILY_RETURN=pl.col("NAV")
            .pct_change()
            .over("TICKER_SYMBOL", order_by="END_DATE")
            * 100
        ).select(
            pl.col("TICKER_SYMBOL"),
            pl.col("END_DATE"),
            pl.col("DAILY_RETURN"),
        )

    def daily_drawdown(self) -> pl.LazyFrame:
        return self.df.with_columns(
            DAILY_DRAWDOWN=(1 - pl.col("NAV") / pl.col("NAV").cum_max()).over(
                "TICKER_SYMBOL", order_by="END_DATE"
            )
            * 100
        ).select(
            pl.col("TICKER_SYMBOL"),
            pl.col("END_DATE"),
            pl.col("DAILY_DRAWDOWN"),
        )

    def cum_return(self) -> pl.LazyFrame:
        """
        计算每个基金的累计收益率*100
        """

        return self.df.group_by("TICKER_SYMBOL").agg(
            CUM_RETURN=(pl.col("NAV").last() / pl.col("NAV").first() - 1) * 100
        )

    def volatility(self) -> pl.LazyFrame:
        return (
            self.daily_return()
            .group_by("TICKER_SYMBOL")
            .agg(VOLATILITY=(pl.col("DAILY_RETURN") / 100).std() * 100)
        )

    # def annual_ret(self):
    #     return self.cum_return().group_by("TICKER_SYMBOL").agg(
    #         ANNUAL_RET=(pl.col("DAILY_RETURN") / 100).mean() * 252 * 100
    #     )

    def max_drawdown(self):
        return (
            self.daily_drawdown()
            .group_by("TICKER_SYMBOL")
            .agg(MAX_DRAWDOWN=pl.col("DAILY_DRAWDOWN").max())
        )

    def max_drawdown_recover(self) -> pl.LazyFrame:
        daily_drawdown = self.daily_drawdown()
        max_drawdown = self.max_drawdown()

        maxdd = daily_drawdown.join(max_drawdown, on="TICKER_SYMBOL", how="left")
        maxdd_date = (
            maxdd.filter(
                (pl.col("DAILY_DRAWDOWN") == pl.col("MAX_DRAWDOWN"))
                & (pl.col("MAX_DRAWDOWN") != 0)
            )
            .group_by("TICKER_SYMBOL")
            .agg(MAXDD_DATE=pl.col("END_DATE").max())
        )
        recover_date = (
            maxdd.join(maxdd_date, on="TICKER_SYMBOL", how="left")
            .filter(
                (pl.col("END_DATE") >= pl.col("MAXDD_DATE"))
                & (pl.col("DAILY_DRAWDOWN") == 0)
            )
            .group_by("TICKER_SYMBOL")
            .agg(MAXDD_RECOVERY_DATE=pl.col("END_DATE").min())
        )
        return (
            max_drawdown.join(maxdd_date, on="TICKER_SYMBOL", how="left")
            .join(recover_date, on="TICKER_SYMBOL", how="left")
            .select(
                [
                    pl.col("TICKER_SYMBOL"),
                    pl.col("MAXDD_DATE"),
                    pl.col("MAXDD_RECOVERY_DATE"),
                    pl.col("MAX_DRAWDOWN"),
                    (pl.col("MAXDD_RECOVERY_DATE") - pl.col("MAXDD_DATE"))
                    .dt.total_days()
                    .fill_null(99999)
                    .alias("MAXDD_RECOVER"),
                ]
            )
        )

    def stats(self) -> pl.LazyFrame:
        result = (
            self.data_name()
            .join(self.cum_return(), on="TICKER_SYMBOL", how="left")
            .join(self.volatility(), on="TICKER_SYMBOL", how="left")
            .join(self.max_drawdown_recover(), on="TICKER_SYMBOL", how="left")
        )
        result = result.select(
            TICKER_SYMBOL=pl.col("TICKER_SYMBOL"),
            START_DATE=pl.col("START_DATE"),
            END_DATE=pl.col("END_DATE"),
            CUM_RETURN=pl.col("CUM_RETURN"),
            ANNUAL_RETURN=(
                (
                    (pl.col("CUM_RETURN") / 100 + 1) ** (365 / pl.col("OPERATION_DAYS"))
                    - 1
                )
                * 100
            ),
            VOLATILITY=pl.col("VOLATILITY"),
            ANNUAL_VOLATILITY=pl.col("VOLATILITY") * np.sqrt(252),
            MAXDD=pl.col("MAX_DRAWDOWN"),
            MAXDD_RECOVER=pl.col("MAXDD_RECOVER"),
            MAXDD_DATE=pl.col("MAXDD_DATE"),
        ).with_columns(
            SHARP_RATIO=pl.col("CUM_RETURN") / pl.col("VOLATILITY"),
            SHARP_RATIO_ANNUAL=pl.col("ANNUAL_RETURN") / pl.col("ANNUAL_VOLATILITY"),
            CALMAR_RATIO_ANNUAL=pl.col("ANNUAL_RETURN") / pl.col("MAXDD"),
        )
        return result


@dataclass
class PerformancePL:
    df: pl.DataFrame | pl.LazyFrame
    start_date: str
    end_date: str

    def __post_init__(self):
        self.start_date = self._parse_date(self.start_date)
        self.end_date = self._parse_date(self.end_date)
        self._prepare_df()

    def _parse_date(self, date: str) -> str:
        """
        解析日期字符串

        Parameters
        ----------
        date : str
            日期字符串

        Returns
        -------
        datetime.datetime
            解析后的日期
        """

        if isinstance(date, str):
            return parse(date)
        if isinstance(date, datetime.datetime):
            return date
        raise ValueError("Invalid date format")

    def _prepare_df(self) -> pl.LazyFrame:
        """
        准备净值数据

        Returns
        -------
        pl.DataFrame
            净值数据
        """
        if isinstance(self.df, pl.DataFrame):
            self.df = self.df.lazy()
        # self.df = _parse_df(self.df)
        # self.df = _filter_df(self.df, self.start_date, self.end_date)
        # self.df = _cal_operation_days(self.df)
        self.df = (
            self.df.pipe(_parse_df)
            .pipe(_filter_df, self.start_date, self.end_date)
            .pipe(_cal_operation_days)
        )
        return self.df

    def stats(self) -> pl.LazyFrame:
        return PerformanceHelper(self.df).stats()


if __name__ == "__main__":

    def get_fund_nav_by_parquet(
        start_date: str, end_date: str, parquet_path: str = "F:/data_parquet/fund_nav/"
    ) -> pl.DataFrame:
        import duckdb

        start_date = parse(start_date)
        end_date = parse(end_date)
        query = f"""
            SELECT 
                END_DATE, 
                TICKER_SYMBOL, 
                ADJ_NAV as NAV
            FROM 
                '{parquet_path}*.parquet'
            where 
                1=1
                and END_DATE between '{start_date}' and '{end_date}'
            order by
                END_DATE,
                TICKER_SYMBOL
        """
        with duckdb.connect() as con:
            df = con.sql(query).pl()
        return df

    def get_fund_nav_by_pl(
        start_date: str, end_date: str, parquet_path: str = "F:/data_parquet/fund_nav/"
    ):
        start_date = parse(start_date)
        end_date = parse(end_date)
        return (
            pl.scan_parquet(f"{parquet_path}*.parquet")
            .select(
                [
                    pl.col("END_DATE"),
                    pl.col("TICKER_SYMBOL"),
                    pl.col("ADJ_NAV").alias("NAV"),
                ]
            )
            .filter(
                (pl.col("END_DATE") >= start_date) & (pl.col("END_DATE") <= end_date)
            )
            .sort(
                by=["END_DATE", "TICKER_SYMBOL"],
            )
        ).collect()

    import time

    # for _ in range(10):
    #     start_time = time.time()
    #     df = get_fund_nav_by_parquet("20220101", "20241119")
    #     end_time = time.time()
    #     print(f"duckdb_read_time: {end_time - start_time}")

    start_time = time.time()
    df = get_fund_nav_by_pl("20231229", "20241121")
    end_time = time.time()
    print(f"polars_read_time: {end_time - start_time}")
    cal_time_list = []
    for _ in range(1):
        start_time = time.time()
        perf_df = PerformancePL(df, "20231229", "20241121")
        perf_df.stats().collect().write_excel("F:/test.xlsx")
        end_time = time.time()
        print(f"Time: {end_time - start_time}")
        cal_time_list.append(end_time - start_time)
