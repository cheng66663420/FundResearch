import datetime

import pandas as pd
import polars as pl

import quant_utils.data_moudle as dm
from data_functions.portfolio_data import get_portfolio_info, query_portfolio_nav
from quant_utils.constant import DATE_FORMAT, DB_CONFIG, TODAY
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.send_email import MailSender

RENAME_DICT = {
    "TICKER_SYMBOL": "组合名称",
    "CYCLE": "周期",
    "START_DATE": "起始日期",
    "END_DATE": "结束日期",
    "INDICATOR": "指标",
    "PORTFOLIO_VALUE": "组合",
    "PEER_RANK": "同类基金排名",
    "BENCHMARK_VALUE_OUTTER": "对客基准",
    "BENCHMARK_VALUE_INNER": "对内基准",
    "PEER_MEDIAN": "同类中位数",
    "PEER_FOF_RANK": "同类FOF排名",
    "PEER_PORTFOLIO_RANK": "同类投顾排名",
}


def crate_database_uri(config: dict) -> str:
    return f"mysql://{config['user']}:{config['pwd']}@{config['host']}:{config['port']}/{config['database']}"


jjtg_uri = crate_database_uri(DB_CONFIG["jjtg"])


def unpivot_dataframe(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    数据透视表转置
    """
    return df.unpivot(
        index=["TICKER_SYMBOL", "START_DATE", "END_DATE"],
        variable_name="INDICATOR",
        value_name="PORTFOLIO_VALUE",
        on=[
            "CUM_RETURN",
            "ANNUAL_RETURN",
            "ANNUAL_VOLATILITY",
            "SHARP_RATIO_ANNUAL",
            "CALMAR_RATIO_ANNUAL",
            "MAXDD",
        ],
    )


def get_portfolio_performance(
    portfolio_name: str,
    end_date: str,
    table_name: str,
) -> pl.LazyFrame:
    query_sql = f"""
    SELECT
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        a.CUM_RETURN,
        a.ANNUAL_RETURN,
        a.ANNUAL_VOLATILITY,
        a.SHARP_RATIO_ANNUAL,
        a.CALMAR_RATIO_ANNUAL,
        a.MAXDD
    FROM
        {table_name} a
    WHERE
        1 = 1
        AND a.END_DATE = '{end_date}'
        AND a.TICKER_SYMBOL = '{portfolio_name}'
    """
    return pl.read_database_uri(query_sql, uri=jjtg_uri).lazy().pipe(unpivot_dataframe)


def get_peer_fund_performance(
    portfolio_name: str,
    end_date: str,
) -> pl.LazyFrame:
    peer_query = (
        dm.get_portfolio_info()
        .query(f"PORTFOLIO_NAME == '{portfolio_name}'")["PEER_QUERY"]
        .values[0]
    )
    peer_query = peer_query.replace("LEVEL", "c.LEVEL")
    peer_query = peer_query.replace("==", "=")
    query_sql = f"""
    SELECT
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        a.CUM_RETURN,
        a.ANNUAL_RETURN,
        a.ANNUAL_VOLATILITY,
        a.SHARP_RATIO_ANNUAL,
        a.CALMAR_RATIO_ANNUAL,
        a.MAXDD 
    FROM
        fund_performance_inner a
        JOIN fund_type_own c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL
    WHERE
        1 = 1 
        and {peer_query}
        AND a.END_DATE = '{end_date}' 
        AND ( 
            c.REPORT_DATE = ( 
                SELECT max( report_date ) 
                FROM fund_type_own 
                WHERE PUBLISH_DATE <= '{end_date}' 
            )
        )
    """
    return pl.read_database_uri(query_sql, uri=jjtg_uri).lazy().pipe(unpivot_dataframe)


def get_peer_fof_performance(
    portfolio_name: str,
    end_date: str,
) -> pl.LazyFrame:
    query_sql = f"""
    SELECT
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        a.CUM_RETURN,
        a.ANNUAL_RETURN,
        a.ANNUAL_VOLATILITY,
        a.SHARP_RATIO_ANNUAL,
        a.CALMAR_RATIO_ANNUAL,
        a.MAXDD 
    FROM
        fund_performance_inner a
        JOIN fof_type c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
    WHERE
        1 = 1 
        AND a.END_DATE = '{end_date}' 
        AND c.INNER_TYPE = '{portfolio_name}' 
    """
    return pl.read_database_uri(query_sql, uri=jjtg_uri).lazy().pipe(unpivot_dataframe)


def get_peer_portfolio_performance(
    portfolio_name: str,
    end_date: str,
):
    query_sql = f"""
    SELECT
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        a.CUM_RETURN,
        a.ANNUAL_RETURN,
        a.ANNUAL_VOLATILITY,
        a.SHARP_RATIO_ANNUAL,
        a.CALMAR_RATIO_ANNUAL,
        a.MAXDD 
    FROM
        peer_performance_inner a
        JOIN peer_portfolio_type c ON c.TICKER_SYMBOL = a.TICKER_SYMBOL 
    WHERE
        1 = 1 
        AND a.END_DATE = '{end_date}' 
        AND c.PORTFOLIO_TYPE = '{portfolio_name}'
    """
    return pl.read_database_uri(query_sql, uri=jjtg_uri).lazy().pipe(unpivot_dataframe)


def get_benchmark_value_outter(portfolio_name: str, end_date: str) -> pl.lazyframe:
    query = f"""
    SELECT
        a.TICKER_SYMBOL,
        a.START_DATE,
        a.END_DATE,
        a.CUM_RETURN,
        a.ANNUAL_RETURN,
        a.ANNUAL_VOLATILITY,
        a.SHARP_RATIO_ANNUAL,
        a.CALMAR_RATIO_ANNUAL,
        a.MAXDD 
    FROM
        benchmark_performance_inner a
    WHERE
        1 = 1 
        AND a.END_DATE = '{end_date}' 
        AND a.TICKER_SYMBOL = '{portfolio_name}' 
    """
    df = pl.read_database_uri(query, uri=jjtg_uri).lazy()
    df_unpivot = df.unpivot(
        index=["TICKER_SYMBOL", "START_DATE", "END_DATE"],
        variable_name="INDICATOR",
        value_name="BENCHMARK_VALUE_OUTTER",
    )
    return df_unpivot


def rank_pct(
    rank_col: str, patition_by: str | list = None, descending: bool = True
) -> pl.Expr:
    rank_expr = pl.col(rank_col).rank(descending=descending).cast(pl.UInt32)
    count_expr = pl.col(rank_col).count().cast(pl.UInt32)
    return 100 * ((rank_expr - 1) / (count_expr - 1)).over(patition_by)


def rank_str(
    rank_col: str, patition_by: str | list = None, descending: bool = True
) -> pl.Expr:
    rank_expr = pl.col(rank_col).rank(descending=descending).cast(pl.UInt32)
    count_expr = pl.col(rank_col).count().cast(pl.UInt32)
    return (
        rank_expr.cast(pl.String).over(patition_by)
        + "/"
        + count_expr.cast(pl.String).over(patition_by)
    )


def _cal_performance_rank_helper(
    df_unpivot: pl.LazyFrame,
    patition_by: str | list = None,
    incicator_list: list = None,
    descending: bool = True,
) -> pl.LazyFrame:
    # 计算排名及百分位
    # 特别注意在polars中rank函数不考虑空值
    result_df = (
        df_unpivot.select(
            [
                pl.col("TICKER_SYMBOL"),
                pl.col("START_DATE"),
                pl.col("END_DATE"),
                pl.col("INDICATOR"),
                pl.col("PORTFOLIO_VALUE"),
            ]
        )
        .filter(pl.col("INDICATOR").is_in(incicator_list))
        .with_columns(
            rank_pct(
                "PORTFOLIO_VALUE", patition_by=patition_by, descending=descending
            ).alias("PEER_RANK_PCT"),
            rank_str(
                "PORTFOLIO_VALUE", patition_by=patition_by, descending=descending
            ).alias("PEER_RANK"),
        )
    )

    return result_df


def cal_performance_rank(df: pl.LazyFrame, portfolio_name: str) -> pl.LazyFrame:
    # df_unpivot = df.unpivot(
    #     index=["TICKER_SYMBOL", "START_DATE", "END_DATE"],
    #     variable_name="INDICATOR",
    #     value_name="PORTFOLIO_VALUE",
    # )
    asscending_indicators = ["MAXDD", "ANNUAL_VOLATILITY"]
    descending_indicators = [
        "CUM_RETURN",
        "ANNUAL_RETURN",
        "SHARP_RATIO_ANNUAL",
        "CALMAR_RATIO_ANNUAL",
    ]
    patition_by = ["START_DATE", "END_DATE", "INDICATOR"]
    # 计算排名及百分位
    # 特别注意在polars中rank函数不考虑空值
    df_asscending = _cal_performance_rank_helper(
        df,
        patition_by=patition_by,
        incicator_list=asscending_indicators,
        descending=False,
    )

    df_descending = _cal_performance_rank_helper(
        df,
        patition_by=patition_by,
        incicator_list=descending_indicators,
        descending=True,
    )
    result = pl.concat([df_asscending, df_descending]).filter(
        pl.col("TICKER_SYMBOL") == portfolio_name
    )
    return result


def get_portfolio_dates(portfolio_name: str, end_date: str) -> pl.LazyFrame:
    query_sql = f"""
    SELECT
        DATE_NAME AS CYCLE,
        START_DATE,
        END_DATE 
    FROM
        portfolio_dates 
    WHERE
        1 = 1 
        AND (PORTFOLIO_NAME = '{portfolio_name}' OR PORTFOLIO_NAME = 'ALL') 
        AND END_DATE = '{end_date}'
    """
    return pl.read_database_uri(query_sql, uri=jjtg_uri).lazy()


def rename_indicator_col_into_chinese(df: pl.LazyFrame):
    indicator_map_dict = {
        "CUM_RETURN": "累计收益率",
        "ANNUAL_RETURN": "年化收益率",
        "ANNUAL_VOLATILITY": "年化波动率",
        "SHARP_RATIO_ANNUAL": "收益波动比",
        "CALMAR_RATIO_ANNUAL": "年化收益回撤比",
        "MAXDD": "最大回撤",
    }
    return df.with_columns(
        pl.col("INDICATOR").replace(indicator_map_dict).alias("INDICATOR")
    )


def add_benchmark_value_otter(
    df: pl.LazyFrame, portfolio_name: str, end_date: str
) -> pl.LazyFrame:
    benchmark_df = get_benchmark_value_outter(portfolio_name, end_date)
    # print("benchmark_df", benchmark_df.collect())
    result = df.join(
        benchmark_df,
        on=["TICKER_SYMBOL", "START_DATE", "END_DATE", "INDICATOR"],
        how="left",
    )
    # print("result", result.collect())
    return result


def add_peer_fof_performance(
    df: pl.LazyFrame, portfolio_name: str, end_date: str
) -> pl.LazyFrame:
    peer_fof = get_peer_fof_performance(portfolio_name, end_date)
    result = cal_performance_rank(pl.concat([df, peer_fof]), portfolio_name)
    result = result.rename(
        {"PEER_RANK_PCT": "PEER_FOF_RANK_PCT", "PEER_RANK": "PEER_FOF_RANK"}
    )
    result = result.select(
        [
            "TICKER_SYMBOL",
            "START_DATE",
            "END_DATE",
            "INDICATOR",
            "PEER_FOF_RANK_PCT",
            "PEER_FOF_RANK",
        ]
    )
    return df.join(
        result, on=["TICKER_SYMBOL", "START_DATE", "END_DATE", "INDICATOR"], how="left"
    )


def add_peer_portfolio_performance(
    df: pl.LazyFrame, portfolio_name: str, end_date: str
) -> pl.LazyFrame:
    peer_portfolio = get_peer_portfolio_performance(portfolio_name, end_date)
    result = cal_performance_rank(pl.concat([df, peer_portfolio]), portfolio_name)
    result = result.rename(
        {"PEER_RANK_PCT": "PEER_PORTFOLIO_RANK_PCT", "PEER_RANK": "PEER_PORTFOLIO_RANK"}
    )
    result = result.select(
        [
            "TICKER_SYMBOL",
            "START_DATE",
            "END_DATE",
            "INDICATOR",
            "PEER_PORTFOLIO_RANK_PCT",
            "PEER_PORTFOLIO_RANK",
        ]
    )
    return df.join(
        result, on=["TICKER_SYMBOL", "START_DATE", "END_DATE", "INDICATOR"], how="left"
    )


def cal_peer_median(peer_fund_performance: pl.LazyFrame) -> pl.LazyFrame:
    peer_median = peer_fund_performance.group_by(
        ["START_DATE", "END_DATE", "INDICATOR"]
    ).agg(pl.col("PORTFOLIO_VALUE").median().alias("PEER_MEDIAN"))
    return peer_median


def _cal_portfolio_performance(
    portfolio_name: str, end_date: str, table_name: str
) -> pl.LazyFrame:
    portfolio_perf = get_portfolio_performance(portfolio_name, end_date, table_name)

    # print("portfolio_perf", portfolio_perf.collect())
    peer_fund_performance = get_peer_fund_performance(portfolio_name, end_date)

    peer_median = cal_peer_median(peer_fund_performance)

    df = pl.concat([portfolio_perf, peer_fund_performance])
    perf_rank = cal_performance_rank(df, portfolio_name).join(
        peer_median, on=["START_DATE", "END_DATE", "INDICATOR"], how="left"
    )
    # perf_rank = portfolio_dates.join(
    #     perf_rank,
    #     on=["START_DATE", "END_DATE"]
    # )
    # print("perf_rank", perf_rank.collect())
    return perf_rank


def get_portfolio_derivatives_rank(portfolio_name: str, end_date: str):
    portfolio_dates = get_portfolio_dates(portfolio_name, end_date)
    perf_rank = _cal_portfolio_performance(
        portfolio_name, end_date, "portfolio_derivatives_performance_inner"
    )
    perf_rank = (
        perf_rank.pipe(add_benchmark_value_otter, portfolio_name, end_date)
        .rename({"BENCHMARK_VALUE_OUTTER": "BENCHMARK_VALUE_INNER"})
        .pipe(rename_indicator_col_into_chinese)
    )
    return portfolio_dates.join(perf_rank, on=["START_DATE", "END_DATE"])


def get_portfolio_rank(portfolio_name: str, end_date: str):
    portfolio_dates = get_portfolio_dates(portfolio_name, end_date)
    perf_rank = _cal_portfolio_performance(
        portfolio_name, end_date, "portfolio_performance_inner"
    )
    perf_rank = (
        perf_rank.pipe(add_benchmark_value_otter, portfolio_name, end_date)
        .pipe(add_peer_fof_performance, portfolio_name, end_date)
        .pipe(add_peer_portfolio_performance, portfolio_name, end_date)
        .pipe(rename_indicator_col_into_chinese)
    )
    return portfolio_dates.join(perf_rank, on=["START_DATE", "END_DATE"])


def update_portfolio_performance(
    start_date: str, end_date: str, portfolio_name_list: list = None
) -> None:
    """
    更新组合表现及衍生组合表现

    Parameters
    ----------
    start_date : str
        开始日期
    end_date : str
        结束日期
    """
    trade_dates = dm.get_period_end_date(
        start_date=start_date, end_date=end_date, period="d"
    )
    portfolio_info = get_portfolio_info()
    portfolio_info["LISTED_DATE"] = portfolio_info["LISTED_DATE"].apply(
        lambda x: x.strftime(DATE_FORMAT)
    )

    for date in trade_dates[0:1]:
        # print(date)
        # 写入自己计算的组合
        portfolio_names = portfolio_info.query(f"LISTED_DATE < '{date}'")[
            "PORTFOLIO_NAME"
        ].tolist()

        if portfolio_name_list is not None:
            portfolio_names = list(set(portfolio_names) & set(portfolio_name_list))
        for portfolio_name in portfolio_names[0:1]:
            portfolio_derivatives_rank = (
                get_portfolio_derivatives_rank(portfolio_name, date)
                .collect()
                .to_pandas()
            )
            DB_CONN_JJTG_DATA.upsert(
                portfolio_derivatives_rank,
                table="portfolio_derivatives_performance",
            )
            print(f"{date}-{portfolio_name}衍生指标写入完成")
            portfolio_rank = (
                get_portfolio_rank(portfolio_name, date).collect().to_pandas()
            )
            DB_CONN_JJTG_DATA.upsert(
                portfolio_rank,
                table="portfolio_performance",
            )
            print(f"{date}-{portfolio_name}正式组合指标写入完成")


def query_portfolio_performance(trade_dt: str):
    query_sql = f"""
        SELECT
            TICKER_SYMBOL,
            CYCLE,
            START_DATE,
            END_DATE,
            INDICATOR,
            PORTFOLIO_VALUE,
            PEER_RANK,
            BENCHMARK_VALUE_OUTTER,
            BENCHMARK_VALUE_INNER,
            PEER_MEDIAN,
            PEER_PORTFOLIO_RANK
        FROM
            portfolio_performance 
        WHERE
            1 = 1 
            AND END_DATE = '{trade_dt}'
    """
    perf_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    trade_2d = dm.offset_trade_dt(trade_dt, 2)
    query_sql = f"""
        SELECT
            TICKER_SYMBOL ,
            CYCLE,
            INDICATOR,
            PEER_FOF_RANK 
        FROM
            portfolio_performance 
        WHERE
            1 = 1 
            AND END_DATE = '{trade_2d}'
        """
    fof_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    perf_df = perf_df.merge(
        fof_df, on=["TICKER_SYMBOL", "CYCLE", "INDICATOR"], how="left"
    )
    perf_df.rename(columns=RENAME_DICT, inplace=True)
    return perf_df


if __name__ == "__main__":
    hour = datetime.datetime.now().hour
    start_date = dm.offset_trade_dt(LAST_TRADE_DT, 2)
    end_date = LAST_TRADE_DT
    update_portfolio_performance(
        start_date=start_date,
        end_date=end_date,
    )
    # # 如果当前时间大于10点，则发送邮件
    # if 11 <= hour <= 15 and dm.if_trade_dt(TODAY):
    #     portfolio_performance = query_portfolio_performance(end_date)
    #     file_path = f"f:/BaiduNetdiskWorkspace/1-基金投研/2.1-监控/2-定时数据/组合监控数据/组合监控数据{end_date}.xlsx"
    #     portfolio_nav = query_portfolio_nav()
    #     with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
    #         portfolio_performance.to_excel(writer, sheet_name="绩效表现")
    #         portfolio_nav.to_excel(writer, sheet_name="组合净值")

    #     mail_sender = MailSender()
    #     mail_sender.message_config(
    #         from_name="进化中的ChenGPT_0.1",
    #         subject=f"【每日监控】投顾组合数据监控{end_date}",
    #         file_path=file_path,
    #         content="详情请见附件",
    #     )
    #     mail_sender.send_mail()
