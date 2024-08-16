# rootPath = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
# sys.path.append(rootPath)
import pandas as pd

import quant_utils.data_moudle as dm
from quant_utils.constant import TODAY
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA


def update_portfolio_products_end_weights(trade_dt: str):
    """
    更具当日净值表现计算当日收盘权重
    Parameters
    ----------
    trade_dt : str
        交易日
    """
    query_sql = """
     INSERT INTO portfolio_derivatives_products_weights ( 
        TRADE_DT, PORTFOLIO_NAME, TICKER_SYMBOL, 
        SEC_SHORT_NAME, START_WEIGHT, RETURN_RATE, LOG_RETURN, 
        WEIGHT_RETURN, END_WEIGHT 
    )
    """
    query_sql += f"""
    WITH c AS ( SELECT TRADE_DT, PORTFOLIO_NAME, TICKER_SYMBOL, WEIGHT AS START_WEIGHT FROM portfolio_products_weights WHERE 1 = 1 AND TRADE_DT = '{trade_dt}' ),
    a AS (
        SELECT
            a.TRADE_DT,
            b.PORTFOLIO_NAME,
            b.TICKER_SYMBOL,
            b.END_WEIGHT AS START_WEIGHT 
        FROM
            md_tradingdaynew a
            JOIN portfolio_derivatives_products_weights b ON a.PREV_TRADE_DATE = b.TRADE_DT 
            LEFT JOIN c ON c.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            and c.PORTFOLIO_NAME IS NULL
            AND a.TRADE_DT = '{trade_dt}' 
            AND a.SECU_MARKET = 83 UNION
        SELECT
            * 
        FROM
            c 
        ),
        e AS (
        SELECT
            a.*,
            ifnull( b.RETURN_RATE_TO_PREV_DT, 0 ) AS RETURN_RATE,
            ln( 1 + ifnull( b.RETURN_RATE_TO_PREV_DT, 0 ) / 100 ) * 100 AS LOG_RETURN,
            a.START_WEIGHT * ifnull( b.RETURN_RATE_TO_PREV_DT, 0 ) / 100 AS WEIGHT_RETURN,
            a.START_WEIGHT * ( 1 + ifnull( b.RETURN_RATE_TO_PREV_DT, 0 ) / 100 ) AS temp_weight 
        FROM
            a
            LEFT JOIN fund_adj_nav b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
            AND a.TRADE_DT = b.END_DATE 
        ),
        b AS ( SELECT TRADE_DT, PORTFOLIO_NAME, sum( temp_weight ) AS sum_weight FROM e GROUP BY TRADE_DT, PORTFOLIO_NAME ) SELECT
        e.TRADE_DT,
        e.PORTFOLIO_NAME,
        e.TICKER_SYMBOL,
        c.SEC_SHORT_NAME,
        e.START_WEIGHT,
        e.RETURN_RATE,
        e.LOG_RETURN,
        e.WEIGHT_RETURN,
        e.temp_weight / b.sum_weight * 100 AS END_WEIGHT 
    FROM
        e
        JOIN b ON e.TRADE_DT = b.TRADE_DT 
        AND e.PORTFOLIO_NAME = b.PORTFOLIO_NAME
        JOIN fund_info c ON c.TICKER_SYMBOL = e.TICKER_SYMBOL 
    WHERE
        1 = 1
    """
    query_sql += """
        ON DUPLICATE KEY UPDATE 
            TRADE_DT =VALUES( TRADE_DT ),
            PORTFOLIO_NAME =VALUES( PORTFOLIO_NAME ),
            TICKER_SYMBOL =VALUES( TICKER_SYMBOL ),
            SEC_SHORT_NAME =VALUES( SEC_SHORT_NAME ),
            START_WEIGHT =VALUES( START_WEIGHT ),
            RETURN_RATE =VALUES( RETURN_RATE ),
            LOG_RETURN =VALUES( LOG_RETURN ),
            WEIGHT_RETURN =VALUES( WEIGHT_RETURN ),
            END_WEIGHT =VALUES(END_WEIGHT)
    """

    DB_CONN_JJTG_DATA.exec_non_query(query_sql)


def update_portfolio_derivatives_ret(trade_dt: str):
    """
    更新组合衍生权重表

    Parameters
    ----------
    trade_dt : str
        交易日
    """

    query_sql = """
        INSERT INTO portfolio_derivatives_ret (
            TRADE_DT,PORTFOLIO_NAME,RETURN_RATE,
            LOG_RETURN_RATE,BENCHMARK_RET_INNER,FUND_MEDIAN_RET,
            LOG_BENCHMARK_RET_INNER,LOG_FUND_MEDIAN_RET,PORTFOLIO_RET_ACCUMULATED,
            BENCHMARK_RET_ACCUMULATED_INNER,FUND_MEDIAN_RET_ACCUMULATED
        )
    """
    query_sql += f"""
    WITH b AS (
        SELECT
            b.PORTFOLIO_NAME,
            b.BENCHMARK_TYPE,
            c.TRADE_DT,
            sum( b.WEIGHT * c.RETURN_RATE / 100 ) AS DAILY_RET 
        FROM
            portfolio_benchmark b
            JOIN portfolio_benchmark_ret c ON b.BENCHMARK_CODE = c.TICKER_SYMBOL 
        WHERE
            1 = 1 
            AND c.TRADE_DT = '{trade_dt}' 
        GROUP BY
            c.TRADE_DT,
            b.PORTFOLIO_NAME,
            b.BENCHMARK_TYPE 
        ),
        c AS (
        SELECT
            TRADE_DT,
            PORTFOLIO_NAME,
            sum( CASE BENCHMARK_TYPE WHEN "TO_CLIENT" THEN DAILY_RET ELSE 0 END )* 100 AS BENCHMARK_RET_INNER,
            sum( CASE BENCHMARK_TYPE WHEN "FUND_MEDIAN" THEN DAILY_RET ELSE 0 END )* 100 AS FUND_MEDIAN_RET 
        FROM
            b 
        GROUP BY
            TRADE_DT,
            PORTFOLIO_NAME 
        ),
        d AS (
        SELECT
            a.TRADE_DT,
            a.PORTFOLIO_NAME,
            sum( a.WEIGHT_RETURN ) AS RETURN_RATE,
            log( 1 + sum( a.WEIGHT_RETURN )/ 100 )* 100 AS LOG_RETURN_RATE,
            ifnull(BENCHMARK_RET_INNER,0) as BENCHMARK_RET_INNER,
            ifnull(FUND_MEDIAN_RET,0) as FUND_MEDIAN_RET,
            log((
                    1+ifnull(BENCHMARK_RET_INNER,0) / 100 
                ))* 100 AS LOG_BENCHMARK_RET_INNER,
            log((
                    1+ifnull(FUND_MEDIAN_RET,0) / 100 
                ))* 100 AS LOG_FUND_MEDIAN_RET 
        FROM
            portfolio_derivatives_products_weights a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME
            LEFT JOIN c ON a.TRADE_DT = c.trade_dt 
            AND c.portfolio_name = a.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT = '{trade_dt}' 
            AND a.TRADE_DT > b.LISTED_DATE -- and a.trade_DT <= ifnull(b.DELISTED_DATE, "2099-12-31")
        GROUP BY
            a.TRADE_DT,
            a.PORTFOLIO_NAME 
        ) SELECT
        d.TRADE_DT,
        d.PORTFOLIO_NAME,
        d.RETURN_RATE,
        d.LOG_RETURN_RATE,
        d.BENCHMARK_RET_INNER,
        d.FUND_MEDIAN_RET,
        d.LOG_BENCHMARK_RET_INNER,
        d.LOG_FUND_MEDIAN_RET,
        ifnull(( d.RETURN_RATE / 100+1 )*( 1+c.PORTFOLIO_RET_ACCUMULATED / 100 )- 1, 0 )* 100 AS PORTFOLIO_RET_ACCUMULATED,
        ifnull(( d.BENCHMARK_RET_INNER / 100 + 1 )*( 1+c.BENCHMARK_RET_ACCUMULATED_INNER / 100 )- 1, 0 )* 100 AS BENCHMARK_RET_ACCUMULATED_INNER,
        ifnull(( d.FUND_MEDIAN_RET / 100 + 1 )*( 1+c.FUND_MEDIAN_RET_ACCUMULATED / 100 )- 1, 0 )* 100 AS FUND_MEDIAN_RET_ACCUMULATED 
    FROM
        d
        JOIN md_tradingdaynew b ON d.TRADE_DT = b.TRADE_DT 
        AND b.SECU_MARKET = 83
        left JOIN portfolio_derivatives_ret c ON b.PREV_TRADE_DATE = c.TRADE_DT 
        AND d.PORTFOLIO_NAME = c.PORTFOLIO_NAME
    """

    query_sql += """
    ON DUPLICATE KEY UPDATE 
        PORTFOLIO_NAME=values(PORTFOLIO_NAME),
        TRADE_DT=values(TRADE_DT),
        RETURN_RATE=values(RETURN_RATE),
        LOG_RETURN_RATE=values(LOG_RETURN_RATE),
        BENCHMARK_RET_INNER=values(BENCHMARK_RET_INNER),
        LOG_BENCHMARK_RET_INNER=values(LOG_BENCHMARK_RET_INNER),
        PORTFOLIO_RET_ACCUMULATED=values(PORTFOLIO_RET_ACCUMULATED),
        BENCHMARK_RET_ACCUMULATED_INNER=values(BENCHMARK_RET_ACCUMULATED_INNER),
        FUND_MEDIAN_RET=values(FUND_MEDIAN_RET),
        LOG_FUND_MEDIAN_RET=values(LOG_FUND_MEDIAN_RET),
        FUND_MEDIAN_RET_ACCUMULATED=values(FUND_MEDIAN_RET_ACCUMULATED)
    """
    DB_CONN_JJTG_DATA.exec_non_query(query_sql)

    # 获取当日是否存在上线的组合
    portfolio_info = dm.get_portfolio_info()
    portfolio_info = portfolio_info.query(f"LISTED_DATE == '{trade_dt}'")
    # 如果组合上线日期，则将收益率初始化为0
    if not portfolio_info.empty:
        temp_list = [
            {
                "TRADE_DT": trade_dt,
                "PORTFOLIO_NAME": portfolio_name,
                "RETURN_RATE": 0,
                "LOG_RETURN_RATE": 0,
                "BENCHMARK_RET_INNER": 0,
                "FUND_MEDIAN_RET": 0,
                "LOG_BENCHMARK_RET_INNER": 0,
                "LOG_FUND_MEDIAN_RET": 0,
                "PORTFOLIO_RET_ACCUMULATED": 0,
                "BENCHMARK_RET_ACCUMULATED_INNER": 0,
                "FUND_MEDIAN_RET_ACCUMULATED": 0,
            }
            for portfolio_name in portfolio_info["PORTFOLIO_NAME"]
        ]
        temp_df = pd.DataFrame(temp_list)
        DB_CONN_JJTG_DATA.upsert(temp_df, "portfolio_derivatives_ret")


def update_portfolio_derivatives_main(trade_dt: str):
    """
    更新组合衍生权重表

    Parameters
    ----------
    trade_dt : str
        交易日
    """
    update_portfolio_products_end_weights(trade_dt)
    update_portfolio_derivatives_ret(trade_dt)
    print(f"{trade_dt}衍生组合权重完成")


if __name__ == "__main__":
    start_date = dm.offset_trade_dt(dm.get_now(), 20)
    end_date = LAST_TRADE_DT
    trade_dates = dm.get_period_end_date(
        start_date=start_date, end_date=end_date, period="d"
    )
    for date in trade_dates:
        print(f"{date}-开始计算")
        update_portfolio_derivatives_main(date)
        print("==" * 40)
