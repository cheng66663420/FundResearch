import datetime
from dataclasses import dataclass

import numpy as np
import polars as pl
from dateutil.parser import parse


def _parse_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    解析净值DataFrame，将END_DATE转换为datetime，TICKER_SYMBOL转换为str，NAV转换为float

    Parameters
    ----------
    df : pl.DataFrame
        净值DataFrame, 包含列: TICKER_SYMBOL, END_DATE, NAV

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
    if not isinstance(df, pl.DataFrame):
        raise ValueError("df must be a polars DataFrame")
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
    df: pl.DataFrame, start_date: datetime.datetime, end_date: datetime.datetime
) -> pl.DataFrame:
    """
    筛选出指定日期范围内的基金，只保留开始日期和结束日期都在指定范围内的基金

    Parameters
    ----------
    df : pl.DataFrame
        净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV
    start_date : datetime.datetime
        开始日期
    end_date : datetime.datetime
        结束日期

    Returns
    -------
    pl.DataFrame
        筛选后的净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV
    """
    return (
        df.lazy()
        .filter(pl.col("END_DATE").is_between(start_date, end_date))
        # 计算每个基金的开始日期和结束日期
        .with_columns(
            START_DATE=pl.col("END_DATE").min().over("TICKER_SYMBOL"),
            MAX_DATE=pl.col("END_DATE").max().over("TICKER_SYMBOL"),
        )
        # 筛选出开始日期和结束日期都在指定范围内的基金
        .filter((pl.col("START_DATE") == start_date) & (pl.col("MAX_DATE") == end_date))
        .select(pl.all().exclude("MAX_DATE"))
    ).collect()


def _cal_operation_days(df: pl.DataFrame) -> pl.DataFrame:
    """
    计算每个基金的操作天数

    Parameters
    ----------
    df : pl.DataFrame
        净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV

    Returns
    -------
    pl.DataFrame
        净值数据，包含列: TICKER_SYMBOL, END_DATE, NAV, OPERATION_DAYS
    """
    return (
        df.lazy()
        # 计算每个基金的下一个交易日的日期
        .with_columns(
            OPERATION_DATE=pl.col("END_DATE")
            .shift(-1)
            .over("TICKER_SYMBOL", order_by="END_DATE")
        )
        # 取最小值
        .with_columns(OPERATION_DATE=pl.col("OPERATION_DATE").min())
        # 计算操作天数
        .with_columns(
            OPERATION_DAYS=(
                (pl.col("END_DATE") - pl.col("OPERATION_DATE")).dt.total_seconds()
                / (60 * 60 * 24)
                + 1
            )
        )
        # 处理操作天数为负数的情况
        .with_columns(
            OPERATION_DAYS=pl.when(pl.col("OPERATION_DAYS") < 0)
            .then(0)
            .otherwise(pl.col("OPERATION_DAYS"))
        )
    ).collect()


@dataclass
class PerformanceHelper:
    df: pl.DataFrame

    def daily_cum_return(self):
        """
        计算每个基金的累计收益率*100
        """
        return (
            pl.col("NAV")
            / pl.col("NAV").first().over("TICKER_SYMBOL", order_by="END_DATE")
            - 1
        ) * 100

    def daily_return(self) -> pl.DataFrame:
        """
        计算每个基金的日收益率*100
        """
        return (
            pl.col("NAV").pct_change().over("TICKER_SYMBOL", order_by="END_DATE") * 100
        )

    def daily_drawdown(self):
        return (
            1
            - pl.col("NAV")
            / pl.col("NAV").cum_max().over("TICKER_SYMBOL", order_by="END_DATE")
        ) * 100

    def std(self, col_name="DAILY_RETURN"):
        return (pl.col(col_name) / 100).std().over(
            "TICKER_SYMBOL", order_by="END_DATE"
        ) * 100

    def annual_ret(self, col_name="CUM_RETURN"):
        return (
            (pl.col(col_name) / 100 + 1) ** (365 / pl.col("OPERATION_DAYS")) - 1
        ) * 100

    def max_drawdown(self, col_name="DAILY_DRAWDOWN"):
        return pl.col(col_name).max().over("TICKER_SYMBOL", order_by="END_DATE")

    def max_drawdown_recover(self):
        maxdd = (
            self.df.with_columns(DAILY_DRAWDOWN=self.daily_drawdown())
            .with_columns(MAX_DRAWDOWN=self.max_drawdown())
            .select(
                pl.col("TICKER_SYMBOL"),
                pl.col("END_DATE"),
                pl.col("DAILY_DRAWDOWN"),
                pl.col("MAX_DRAWDOWN"),
            )
        )

        maxdd_date = maxdd.filter(
            (pl.col("DAILY_DRAWDOWN") == pl.col("MAX_DRAWDOWN"))
            & (pl.col("MAX_DRAWDOWN") != 0)
        ).select(
            pl.col("TICKER_SYMBOL"),
            pl.col("END_DATE").max().over("TICKER_SYMBOL").alias("MAXDD_DATE"),
        )

        recover_date = (
            maxdd.join(maxdd_date, on="TICKER_SYMBOL", how="left")
            .filter(
                (pl.col("END_DATE") >= pl.col("MAXDD_DATE"))
                & (pl.col("DAILY_DRAWDOWN") == 0)
            )
            .with_columns(
                [
                    pl.col("END_DATE")
                    .min()
                    .over("TICKER_SYMBOL")
                    .alias("MIN_DD_RECOVERY_DATE")
                ]
            )
            # 筛选出最小的回撤修复日期
            .filter((pl.col("END_DATE") == pl.col("MIN_DD_RECOVERY_DATE")))
            # 计算最大回撤修复天数
            .select(
                pl.col("TICKER_SYMBOL"),
                pl.col("MAXDD_DATE"),
                pl.col("MIN_DD_RECOVERY_DATE").alias("RECOVER_DATE"),
                (
                    (
                        pl.col("MIN_DD_RECOVERY_DATE") - pl.col("MAXDD_DATE")
                    ).dt.total_seconds()
                    / (60 * 60 * 24)
                ).alias("MAXDD_RECOVER"),
            )
        )
        return (
            maxdd_date.join(recover_date, on="TICKER_SYMBOL", how="left").with_columns(
                [pl.col("MAXDD_RECOVER").fill_null(99999)]
            )
        ).lazy()

    def _before_stats(self):
        maxdd = self.max_drawdown_recover()

        end_date_df = (
            self.df.select(
                pl.col("TICKER_SYMBOL"),
                pl.col("START_DATE").min().over("TICKER_SYMBOL").alias("START_DATE"),
                pl.col("END_DATE").max().over("TICKER_SYMBOL").alias("END_DATE"),
            )
            .unique()
            .lazy()
        )
        return (
            self.df.lazy()
            .with_columns(
                CUM_RETURN=self.daily_cum_return(),
                DAILY_RETURN=self.daily_return(),
                DAILY_DRAWDOWN=self.daily_drawdown(),
            )
            .with_columns(
                ANNUAL_RETURN=self.annual_ret(),
                MAX_DRAWDOWN=self.max_drawdown(),
                VOLATILITY=self.std(),
                ANNUAL_VOLATILITY=self.std() * np.sqrt(252),
            )
            .join(
                end_date_df, on=["TICKER_SYMBOL", "END_DATE", "START_DATE"], how="right"
            )
            .join(maxdd, on="TICKER_SYMBOL", how="left")
        ).collect()

    def stats(self):
        # print(self._before_stats())
        return self._before_stats().select(
            [
                pl.col("TICKER_SYMBOL"),
                pl.col("START_DATE"),
                pl.col("END_DATE"),
                pl.col("CUM_RETURN"),
                pl.col("ANNUAL_RETURN"),
                pl.col("VOLATILITY"),
                pl.col("ANNUAL_VOLATILITY"),
                (pl.col("CUM_RETURN") / pl.col("VOLATILITY")).alias("SHARP_RATIO"),
                (pl.col("ANNUAL_RETURN") / pl.col("ANNUAL_VOLATILITY")).alias(
                    "SHARP_RATIO_ANNUAL"
                ),
                pl.col("MAX_DRAWDOWN").alias("MAXDD"),
                (pl.col("ANNUAL_RETURN") / pl.col("MAX_DRAWDOWN")).alias(
                    "CALMAR_RATIO_ANNUAL"
                ),
                pl.col("MAXDD_DATE"),
                pl.col("MAXDD_RECOVER"),
                pl.col("RECOVER_DATE"),
            ]
        )


@dataclass
class PerformancePL:
    df: pl.DataFrame
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

    def _prepare_df(self) -> pl.DataFrame:
        """
        准备净值数据

        Returns
        -------
        pl.DataFrame
            净值数据
        """
        self.df = _parse_df(self.df)
        self.df = _filter_df(self.df, self.start_date, self.end_date)
        self.df = _cal_operation_days(self.df)

    def stats(self):
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
        df = (
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
        return df

    import time

    for _ in range(10):
        start_time = time.time()
        df = get_fund_nav_by_parquet("20220101", "20241119")
        end_time = time.time()
        print(f"duckdb_read_time: {end_time - start_time}")
    for _ in range(10):
        start_time = time.time()
        df = get_fund_nav_by_pl("20220101", "20241119")
        end_time = time.time()
        print(f"polars_read_time: {end_time - start_time}")
    cal_time_list = []
    for _ in range(10):
        start_time = time.time()
        perf_df = PerformancePL(df, "20231229", "20241119").stats()
        end_time = time.time()
        print(f"Time: {end_time - start_time}")
        cal_time_list.append(end_time - start_time)
