import feather
import polars as pl
from fund_db_updates import update_derivatives_db
from fund_db_updates_jy import update_jy_db
from fund_db_updates_new import update_derivatives_jy

import quant_utils.data_moudle as dm
from quant_utils.constant import TODAY
from quant_utils.db_conn import DB_CONN_JJTG_DATA
import pandas as pd


def write_nav_into_ftr(date: str, file_path: str = "F:/data_ftr/fund_nav/") -> None:
    query_sql = f"""
        SELECT
            END_DATE,
            TICKER_SYMBOL,
            UNIT_NAV,
            ADJ_NAV,
            ADJ_FACTOR,
            RETURN_RATE,
            RETURN_RATE_TO_PREV_DT,
            LOG_RET,
            UPDATE_TIME
        FROM
            `fund_adj_nav` 
        WHERE
            1 = 1 
            AND END_DATE = '{date}'
    """
    nav_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    nav_df["END_DATE"] = pd.to_datetime(nav_df["END_DATE"])
    if not nav_df.empty:
        feather.write_dataframe(nav_df, f"{file_path}{date}.ftr")
        nav_df.to_parquet(f"F:/data_parquet/fund_nav/{date}.parquet")


def write_trade_dt():
    """
    每日更新交易日历表
    """
    query_sql = """
    SELECT
        TRADE_DT,
        IF_TRADING_DAY,
        PREV_TRADE_DATE,
        WEEK_END_DATE,
        MONTH_END_DATE,
        QUARTER_END_DATE,
        YEAR_END_DATE,
        IF_WEEK_END,
        IF_MONTH_END,
        IF_QUARTER_END,
        IF_YEAR_END 
    FROM
        md_tradingdaynew 
    WHERE
        1 = 1 
        AND `SECU_MARKET` = '83' 
        AND TRADE_DT <= DATE_ADD( CURRENT_DATE, INTERVAL 365 DAY ) 
    ORDER BY
        TRADE_DT
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df.to_parquet("F:/data_parquet/trade_cal.parquet")


def update_perf_desc():
    df = (
        pl.scan_parquet("F:/data_parquet/fund_nav/*.parquet")
        .group_by("TICKER_SYMBOL")
        .agg(NAV_END_DATE=pl.col("END_DATE").max())
    )
    df = df.collect().to_pandas()
    DB_CONN_JJTG_DATA.upsert(df, "fund_perf_desc")


def main():
    # update_wind_db()
    update_jy_db()
    update_derivatives_jy()
    update_derivatives_db()
    n_days_before = dm.offset_trade_dt(TODAY, 5)
    for date in dm.get_trade_cal(n_days_before, TODAY):
        write_nav_into_ftr(date)
    write_trade_dt()
    update_perf_desc()


if __name__ == "__main__":
    main()
