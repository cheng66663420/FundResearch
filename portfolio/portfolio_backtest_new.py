import time

import numpy as np
import pandas as pd
from dateutil.parser import parse
from joblib import Parallel, delayed
from pandarallel import pandarallel

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.performance import Performance, periods_performance


class PortfolioBacktest:
    def __init__(
        self,
        test_name_list: list,
        end_date: str = None,
        benchmark_inner="930950",
        benckmark_fund="主动权益-A股主题-无主题",
        portfolio_num=10,
    ) -> None:
        test_name_list = [str(i) for i in test_name_list]
        self.test_name_list = ",".join(test_name_list)
        self.portfolio_num = portfolio_num

        self.end_date = (
            dm.offset_trade_dt(dm.get_now(), 1) if end_date is None else end_date
        )
        self.benchmark_inner = benchmark_inner
        self.benckmark_fund = benckmark_fund

    def __get_test_info(self) -> pd.DataFrame:
        query_sql = f"""
        SELECT
            TEST_CODE,
            TEST_NAME,
            TEST_SPACE,
            CYCLE,
            DATE_FORMAT( IFNULL( START_DATE, "20991231" ), '%Y%m%d' ) as START_DATE,
            DATE_FORMAT( IFNULL( END_DATE, "19900101" ), '%Y%m%d' ) as END_DATE,
            PORTFOLIO_NUM
        FROM
            porfolio_backtest_list 
        WHERE
            1 = 1
            AND TEST_CODE IN ({self.test_name_list})
        """
        return DB_CONN_JJTG_DATA.exec_query(query_sql)

    def __update_start_end_dates(self):
        query_sql = f"""
        INSERT INTO porfolio_backtest_list ( TEST_CODE, START_DATE, END_DATE, PORTFOLIO_NUM) 
        WITH a AS ( SELECT TEST_CODE, max( TRADE_DT ) AS END_DATE FROM portfolio_backtest_products_weights GROUP BY TEST_CODE ),
        b AS ( SELECT TEST_CODE, min( TRADE_DT ) AS START_DATE, max( PORTFOLIO_NAME ) AS PORTFOLIO_NUM FROM portfolio_backtest_weights GROUP BY TEST_CODE ) SELECT
        c.TEST_CODE,
        b.START_DATE,
        a.END_DATE,
        b.PORTFOLIO_NUM 
        FROM
            porfolio_backtest_list c
            LEFT JOIN a ON c.TEST_CODE = a.TEST_CODE
            LEFT JOIN b ON c.TEST_CODE = b.TEST_CODE 
        WHERE
            1 = 1 
            AND c.TEST_CODE IN ({self.test_name_list})
        ON DUPLICATE KEY UPDATE 
            START_DATE =VALUES( START_DATE ),
            END_DATE =VALUES(END_DATE),
            PORTFOLIO_NUM = VALUES(PORTFOLIO_NUM)
        """
        DB_CONN_JJTG_DATA.exec_non_query(query_sql)

    def __update_portfolio_backtest_products_start_weights(self, trade_dt: str):
        """
        更新组合产品初始权重，逻辑如下
        (1)组合如果当日有调仓则取调仓权重
        (2)没有调仓则取前一日结束权重
        Parameters
        ----------
        trade_dt : str
            交易日
        """

        query_sql = """
        INSERT INTO portfolio_backtest_products_weights ( 
            TEST_CODE, TRADE_DT, PORTFOLIO_NAME, TICKER_SYMBOL, 
            SEC_SHORT_NAME, START_WEIGHT, RETURN_RATE, LOG_RETURN, 
            WEIGHT_RETURN, END_WEIGHT )
        """
        query_sql += f"""
        WITH c AS ( 
            SELECT 
                a.TEST_CODE, a.TRADE_DT, a.PORTFOLIO_NAME, a.TICKER_SYMBOL, 
                a.START_WEIGHT 
            FROM 
                portfolio_backtest_weights a
                join porfolio_backtest_list b on b.TEST_CODE = a.TEST_CODE
            WHERE 
                1 = 1 
                AND a.TRADE_DT = '{trade_dt}' 
                AND b.TEST_CODE in ({self.test_name_list})
        ),
        a AS (
            SELECT
                b.TEST_CODE,
                a.TRADE_DT,
                b.PORTFOLIO_NAME,
                b.TICKER_SYMBOL,
                b.END_WEIGHT AS START_WEIGHT 
            FROM
                md_tradingdaynew a
                JOIN portfolio_backtest_products_weights b ON a.PREV_TRADE_DATE = b.TRADE_DT 
                left join c on c.TEST_CODE=b.TEST_CODE and c.PORTFOLIO_NAME = b.PORTFOLIO_NAME
                JOIN porfolio_backtest_list d ON d.TEST_CODE = b.TEST_CODE  
            WHERE
                1 = 1 
                AND d.TEST_CODE IN ({self.test_name_list})
                and c.TEST_CODE is null 
                and c.PORTFOLIO_NAME is null 
                AND a.TRADE_DT = '{trade_dt}' 
                AND a.SECU_MARKET = 83 UNION
            SELECT
                * 
            FROM
                c
        ),
        e as (
            select
                a.*,
                ifnull(b.RETURN_RATE_TO_PREV_DT, 0) AS RETURN_RATE,
                ln( 1 + ifnull(b.RETURN_RATE_TO_PREV_DT,0) / 100 ) * 100 AS LOG_RETURN,
                a.START_WEIGHT * ifnull(b.RETURN_RATE_TO_PREV_DT,0) / 100 AS WEIGHT_RETURN,
                a.START_WEIGHT * ( 1 + ifnull(b.RETURN_RATE_TO_PREV_DT,0) / 100 ) AS temp_weight 
            FROM
                a
                left JOIN fund_adj_nav b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
                AND a.TRADE_DT = b.END_DATE
        ), 
        b AS ( 
            SELECT 
                TEST_CODE, TRADE_DT, PORTFOLIO_NAME, 
                sum( temp_weight ) AS sum_weight 
            FROM 
                e 
            GROUP BY 
                TEST_CODE, TRADE_DT, PORTFOLIO_NAME 
        ) SELECT
            e.TEST_CODE,
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
            and e.TEST_CODE = b.TEST_CODE
            join fund_info c on c.TICKER_SYMBOL = e.TICKER_SYMBOL
        WHERE
            1 = 1
        # """
        query_sql += """
        ON DUPLICATE KEY UPDATE 
            # TEST_CODE =VALUES( TEST_CODE ),
            # TRADE_DT =VALUES( TRADE_DT ),
            # PORTFOLIO_NAME =VALUES( PORTFOLIO_NAME ),
            # TICKER_SYMBOL =VALUES( TICKER_SYMBOL ),
            SEC_SHORT_NAME =VALUES( SEC_SHORT_NAME ),
            START_WEIGHT =VALUES( START_WEIGHT ),
            RETURN_RATE =VALUES( RETURN_RATE ),
            LOG_RETURN =VALUES( LOG_RETURN ),
            WEIGHT_RETURN =VALUES( WEIGHT_RETURN ),
            END_WEIGHT =VALUES(END_WEIGHT)
        """
        DB_CONN_JJTG_DATA.exec_non_query(query_sql)

    def __update_portfolio_derivatives_ret(self, trade_dt: str):
        """
        更新组合衍生权重表

        Parameters
        ----------
        trade_dt : str
            交易日
        """
        query_sql = """
        INSERT INTO portfolio_backtest_ret (
            TEST_CODE,TRADE_DT,PORTFOLIO_NAME,RETURN_RATE,
            LOG_RETURN_RATE,BENCHMARK_RET_INNER,FUND_MEDIAN_RET,
            LOG_BENCHMARK_RET_INNER,LOG_FUND_MEDIAN_RET,PORTFOLIO_RET_ACCUMULATED,
            BENCHMARK_RET_ACCUMULATED_INNER,FUND_MEDIAN_RET_ACCUMULATED
        )
        """
        query_sql += f"""
        WITH 
        c as (
        SELECT
            TRADE_DT,
            sum( CASE TICKER_SYMBOL WHEN "{self.benchmark_inner}" THEN RETURN_RATE ELSE 0 END ) AS BENCHMARK_RET_INNER,
            sum( CASE TICKER_SYMBOL WHEN "{self.benckmark_fund}" THEN RETURN_RATE ELSE 0 END ) AS FUND_MEDIAN_RET 
        FROM
            portfolio_benchmark_ret 
        where
            TRADE_DT = '{trade_dt}' 
        GROUP BY
	        TRADE_DT
        ),b as (
        SELECT
            a.TEST_CODE,
            date_format(a.TRADE_DT, "%Y%m%d") as TRADE_DT,
            a.PORTFOLIO_NAME,
            sum( a.WEIGHT_RETURN ) AS RETURN_RATE,
            ln( 1 + sum( a.WEIGHT_RETURN )/ 100 )* 100 AS LOG_RETURN_RATE,
            BENCHMARK_RET_INNER,
            FUND_MEDIAN_RET,
            ln(( 1+BENCHMARK_RET_INNER / 100 ))* 100 AS LOG_BENCHMARK_RET_INNER, 
            ln(( 1+FUND_MEDIAN_RET / 100 ))* 100 AS LOG_FUND_MEDIAN_RET  
        FROM
            portfolio_backtest_products_weights a
            LEFT JOIN c ON a.TRADE_DT = c.trade_dt
            join porfolio_backtest_list d on d.TEST_CODE = a.TEST_CODE
        WHERE
            1 = 1 
            AND a.TRADE_DT = '{trade_dt}' 
            AND d.TEST_CODE in ({self.test_name_list})
        GROUP BY
            a.TEST_CODE,
            a.TRADE_DT,
            a.PORTFOLIO_NAME
        )
        SELECT
            b.TEST_CODE,
            b.TRADE_DT,
            b.PORTFOLIO_NAME,
            b.RETURN_RATE,
            b.LOG_RETURN_RATE,
            b.BENCHMARK_RET_INNER,
            b.FUND_MEDIAN_RET,
            b.LOG_BENCHMARK_RET_INNER,
            b.LOG_FUND_MEDIAN_RET,
            ifnull(( c.PORTFOLIO_RET_ACCUMULATED / 100+1 )*( 1+b.RETURN_RATE / 100 )- 1, 0 )* 100 AS PORTFOLIO_RET_ACCUMULATED,
            ifnull(( b.BENCHMARK_RET_INNER / 100+1 )*( 1+c.BENCHMARK_RET_ACCUMULATED_INNER / 100 )- 1, 0 )* 100 AS BENCHMARK_RET_ACCUMULATED_INNER,
            ifnull(( b.FUND_MEDIAN_RET / 100+1 )*( 1+c.FUND_MEDIAN_RET_ACCUMULATED / 100 )- 1, 0 )* 100 AS FUND_MEDIAN_RET_ACCUMULATED 
        FROM
            b
            JOIN md_tradingdaynew a ON a.TRADE_DT = b.TRADE_DT 
            AND a.SECU_MARKET = 83
            JOIN portfolio_backtest_ret c ON a.PREV_TRADE_DATE = c.TRADE_DT 
            AND b.PORTFOLIO_NAME = c.PORTFOLIO_NAME 
            and b.TEST_CODE = c.TEST_CODE
        """
        query_sql += """
        ON DUPLICATE KEY UPDATE 
            # TEST_NAME=values(TEST_NAME),
            # PORTFOLIO_NAME=values(PORTFOLIO_NAME),
            # TRADE_DT=values(TRADE_DT),
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

        temp_df = self.test_info.query(f"START_DATE == '{trade_dt}'")
        if not temp_df.empty:
            temp_list = [
                {
                    "TEST_CODE": portfolio["TEST_CODE"],
                    "TRADE_DT": trade_dt,
                    "PORTFOLIO_NAME": str(i).rjust(2, "0"),
                    "RETURN_RATE": 0.0,
                    "LOG_RETURN_RATE": 0.0,
                    "BENCHMARK_RET_INNER": 0.0,
                    "PORTFOLIO_RET_ACCUMULATED": 0.0,
                    "BENCHMARK_RET_ACCUMULATED_INNER": 0.0,
                    "FUND_MEDIAN_RET": 0.0,
                    "LOG_FUND_MEDIAN_RET": 0.0,
                    "FUND_MEDIAN_RET_ACCUMULATED": 0.0,
                    "LOG_BENCHMARK_RET_INNER": 0.0,
                }
                for _, portfolio in temp_df.iterrows()
                for i in range(1, 1 + portfolio["PORTFOLIO_NUM"])
            ]
            result = pd.DataFrame(temp_list)
            DB_CONN_JJTG_DATA.upsert(result, table="portfolio_backtest_ret")

    def update_backtest(self) -> None:
        """
        更新回测持仓数据
        """
        self.__update_start_end_dates()
        self.test_info = self.__get_test_info()
        if self.test_info.empty:
            print("没有持仓数据")
            return None
        start_date_min = self.test_info["START_DATE"].min()
        end_date_min = self.test_info["END_DATE"].min()
        start_date = max(start_date_min, end_date_min)

        trade_dts = dm.get_period_end_date(
            start_date=start_date, end_date=self.end_date, period="d"
        )

        t1 = time.time()
        for trade_dt in trade_dts:
            print(trade_dt)
            self.__update_portfolio_backtest_products_start_weights(trade_dt)
            self.__update_portfolio_derivatives_ret(trade_dt)
        t2 = time.time()
        print(f"用时{t2-t1}s")
        self.__update_start_end_dates()

    def delete_date(self):
        table_list = [
            "portfolio_backtest_ret",
            "portfolio_backtest_products_weights",
            "portfolio_backtest_weights",
        ]
        for table in table_list:
            query_sql = f"""
            delete from {table} where TEST_CODE in ({self.test_name_list})
            """
            DB_CONN_JJTG_DATA.exec_non_query(query_sql)

    def ruin(self):
        """
        毁灭函数，慎重使用
        """
        table_list = [
            "portfolio_backtest_ret",
            "portfolio_backtest_products_weights",
            "portfolio_backtest_weights",
        ]
        for table in table_list:
            query_sql = f"""
            TRUNCATE TABLE {table} 
            """
            DB_CONN_JJTG_DATA.exec_non_query(query_sql)

    def get_backtest_nav(self):
        query = f"""
        select
            a.TEST_CODE,
            b.TEST_NAME,
            a.PORTFOLIO_NAME,
            date_format(a.TRADE_DT, "%Y%m%d") as TRADE_DT,
            a.LOG_RETURN_RATE,
            a.PORTFOLIO_RET_ACCUMULATED/100 + 1 as NAV,
            a.BENCHMARK_RET_ACCUMULATED_INNER/100 + 1 as BENCHMARK_NAV,
            a.FUND_MEDIAN_RET_ACCUMULATED/100 + 1 as FUND_MEDIAN_NAV,
            (a.PORTFOLIO_RET_ACCUMULATED - a.BENCHMARK_RET_ACCUMULATED_INNER)/100 + 1 as ALPHA
        from 
            portfolio_backtest_ret a 
            join porfolio_backtest_list b on a.TEST_CODE = b.TEST_CODE
        where 
            1=1 
            and b.TEST_CODE in ({self.test_name_list})
        order by 
            TEST_CODE, TRADE_DT, PORTFOLIO_NAME
        """
        return DB_CONN_JJTG_DATA.exec_query(query)

    def performance(self, if_periods=0):
        ret_df = self.get_backtest_nav().set_index("TRADE_DT")
        result = []
        for portfolio_name, df in ret_df.groupby(
            by=["TEST_CODE", "TEST_NAME", "PORTFOLIO_NAME"]
        ):
            if if_periods:
                perf = periods_performance(df["NAV"], df["BENCHMARK_NAV"])
            else:
                perf = Performance(df["NAV"], df["BENCHMARK_NAV"]).stats().T
            perf["TEST_CODE"] = portfolio_name[0]
            perf["TEST_NAME"] = portfolio_name[1]
            perf["PORTFOLIO_NAME"] = portfolio_name[2]
            result.append(perf)
        return pd.concat(result)


if __name__ == "__main__":
    indictaor_list = list(range(1, 27))
    test = PortfolioBacktest(indictaor_list)
    test.update_backtest()
