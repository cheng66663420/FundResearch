import feather
from fund_db_updates import update_derivatives_db
from fund_db_updates_jy import update_jy_db
from fund_db_updates_new import update_derivatives_jy
from fund_db_updates_wind import update_wind_db

import quant_utils.data_moudle as dm
from quant_utils.constant import TODAY
from quant_utils.db_conn import DB_CONN_JJTG_DATA


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
    if not nav_df.empty:
        feather.write_dataframe(nav_df, f"{file_path}{date}.ftr")


if __name__ == "__main__":
    # update_wind_db()
    update_jy_db()
    update_derivatives_jy()
    update_derivatives_db()
    n_days_before = dm.offset_trade_dt(TODAY, 5)
    for date in dm.get_trade_cal(n_days_before, TODAY):
        write_nav_into_ftr(date)
