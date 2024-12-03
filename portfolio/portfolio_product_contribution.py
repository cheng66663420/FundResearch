import numpy as np
import pandas as pd

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA

# rootPath = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
# sys.path.append(rootPath)


DB_CONN = DB_CONN_JJTG_DATA


def get_fund_own_type(date: str = "20991231", level_num: int = 3):
    query_sql = f"""
    SELECT
        TICKER_SYMBOL,
        LEVEL_{level_num} 
    FROM
        fund_type_own_temp 
    WHERE
        REPORT_DATE = (
        SELECT
            max( REPORT_DATE ) 
        FROM
            fund_type_own_temp 
    WHERE
        REPORT_DATE <= '{date}')
    """
    return DB_CONN.exec_query(query_sql)


def get_position_change_dates(
    portfolio_name: str, start_date: str, end_date: str
) -> list:
    """
    获取组合区间时间内部的调仓生效日，区间为【start_date, end_date)

    Parameters
    ----------
    portfolio_name : str
        _description_
    start_date : str
        起始时间
    end_date : str
        结束时间

    Returns
    -------
    list
        调仓生效日列表
    """
    query_sql = f"""
    SELECT DISTINCT
        DATE_FORMAT(TRADE_DT, "%Y%m%d") as TRADE_DT
    FROM
        portfolio_products_weights 
    WHERE
        1 = 1 
        AND TRADE_DT > '{start_date}' 
        AND TRADE_DT <= '{end_date}' 
        AND PORTFOLIO_NAME = '{portfolio_name}' 
    ORDER BY
        TRADE_DT
    """
    result = DB_CONN.exec_query(query_sql)
    return result["TRADE_DT"].tolist()


def get_single_period_products_contribution(
    portfolio_name: str, start_date: str, end_date: str, level_num: int = 3
) -> pd.DataFrame:
    # start_date = dm.offset_period_trade_dt(start_date, 1)
    # end_date = dm.offset_period_trade_dt(end_date, 1)
    query_sql = f"""
    WITH a AS (
        SELECT
            PORTFOLIO_NAME,
            TICKER_SYMBOL,
            min( TRADE_DT ) AS START_TRADE_DT,
            max( TRADE_DT ) AS END_TRADE_DT 
        FROM
            portfolio_derivatives_products_weights 
        WHERE
            1 = 1 
            AND TRADE_DT >=  "{start_date}"
            AND TRADE_DT < "{end_date}"
            AND `PORTFOLIO_NAME` = "{portfolio_name}"
        GROUP BY
            PORTFOLIO_NAME,
            TICKER_SYMBOL 
        ),
        b AS (
        SELECT
            a.PORTFOLIO_NAME,
            a.TICKER_SYMBOL,
            b.SEC_SHORT_NAME,
            a.START_TRADE_DT,
            a.END_TRADE_DT,
            b.START_WEIGHT,
            b.END_WEIGHT
        FROM
            a
            JOIN portfolio_derivatives_products_weights b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
            AND a.TICKER_SYMBOL = b.TICKER_SYMBOL 
            AND a.START_TRADE_DT = b.TRADE_DT 
        ),
        c AS (
        SELECT
            PORTFOLIO_NAME,
            TICKER_SYMBOL,
            ( exp( sum( LOG_RETURN )/ 100 )- 1 )* 100 AS SUM_PERIOD_RETURN 
        FROM
            portfolio_derivatives_products_weights 
        WHERE
            1 = 1 
            AND TRADE_DT >=  "{start_date}"
            AND TRADE_DT < "{end_date}"
            AND `PORTFOLIO_NAME` = "{portfolio_name}"
        GROUP BY
            PORTFOLIO_NAME,
            TICKER_SYMBOL
        ),
        d as (
        SELECT
            a.PORTFOLIO_NAME,
            a.TICKER_SYMBOL,
            ( exp( sum( ifnull(b.LOG_ALPHA_LEVEL_{level_num},0) )/ 100 )- 1 )* 100 AS SUM_PERIOD_ALPHA_LEVEL_{level_num} 
        FROM
            portfolio_derivatives_products_weights a
            left JOIN fund_derivatives_fund_log_alpha b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
            AND a.TRADE_DT = b.END_DATE 
        WHERE
            1 = 1 
            AND TRADE_DT >=  "{start_date}"
            AND TRADE_DT < "{end_date}"
            AND `PORTFOLIO_NAME` = "{portfolio_name}"
        GROUP BY
            PORTFOLIO_NAME,
            TICKER_SYMBOL
        ) SELECT
        b.PORTFOLIO_NAME,
        b.TICKER_SYMBOL,
        e.SEC_SHORT_NAME,
        b.START_TRADE_DT,
        b.END_TRADE_DT,
        b.START_WEIGHT,
        b.END_WEIGHT,
        c.SUM_PERIOD_RETURN,
        b.START_WEIGHT * c.SUM_PERIOD_RETURN / 100 AS "PERIOD_RETURN_CONTRIBUTION",
        d.SUM_PERIOD_ALPHA_LEVEL_{level_num},
        b.START_WEIGHT * d.SUM_PERIOD_ALPHA_LEVEL_{level_num}/100 as "PERIOD_ALPHA_CONTRIBUTION"
    FROM
        b
        JOIN c ON c.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        AND c.TICKER_SYMBOL = b.TICKER_SYMBOL
        JOIN d ON d.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
	    AND d.TICKER_SYMBOL = b.TICKER_SYMBOL
        join fund_info e on e.Ticker_symbol = b.TICKER_SYMBOL
    """
    # print(query_sql)
    return DB_CONN.exec_query(query_sql)


def cal_periods_prodcuts_contribution(
    portfolio_name: str, start_date: str, end_date: str, level_num: int = 3
):
    # print(start_date, end_date)
    start_date = dm.offset_period_trade_dt(start_date, 1)
    end_date = dm.offset_period_trade_dt(end_date, 1)
    query_sql = """
    SELECT
        * 
    FROM
        portfolio_info 
    WHERE
        1 = 1 
    ORDER BY
        IF_LISTED DESC,
        ID
    """

    portfolio_info = DB_CONN_JJTG_DATA.exec_query(query_sql)
    portfolio_info["LISTED_DATE"] = portfolio_info["LISTED_DATE"].apply(
        lambda s: s.strftime("%Y%m%d") if s else ""
    )
    portfolio_info["TO_CLIENT_DATE"] = portfolio_info["TO_CLIENT_DATE"].apply(
        lambda s: s.strftime("%Y%m%d") if s else dm.get_now()
    )
    portfolio_info["DELISTED_DATE"] = portfolio_info["DELISTED_DATE"].apply(
        lambda s: s.strftime("%Y%m%d") if s else dm.get_now()
    )
    listed_date = portfolio_info.query(f"PORTFOLIO_NAME == '{portfolio_name}'")[
        "LISTED_DATE"
    ].values[0]
    delisted_date = portfolio_info.query(f"PORTFOLIO_NAME == '{portfolio_name}'")[
        "DELISTED_DATE"
    ].values[0]

    start_date = max(dm.offset_period_trade_dt(listed_date, 1), start_date)
    end_date = min(dm.offset_period_trade_dt(delisted_date, 1), end_date)

    position_change_dates = get_position_change_dates(
        portfolio_name, start_date, end_date
    )
    # print(position_change_dates)
    dates_list = [start_date] + position_change_dates + [end_date]

    result_list = []
    for i in range(len(dates_list) - 1):
        df_temp = get_single_period_products_contribution(
            portfolio_name, dates_list[i], dates_list[i + 1], level_num=level_num
        )
        result_list.append(df_temp)
        if i == 0:
            df_start = df_temp.copy()[
                ["PORTFOLIO_NAME", "TICKER_SYMBOL", "START_TRADE_DT", "START_WEIGHT"]
            ]
            # print(df_start)
        if i == len(dates_list) - 2:
            df_end = df_temp.copy()[
                ["PORTFOLIO_NAME", "TICKER_SYMBOL", "END_TRADE_DT", "END_WEIGHT"]
            ]
            # print(df_end)

    result = pd.concat(result_list)
    result = result[
        [
            "PORTFOLIO_NAME",
            "TICKER_SYMBOL",
            "SEC_SHORT_NAME",
            "SUM_PERIOD_RETURN",
            "PERIOD_RETURN_CONTRIBUTION",
            f"SUM_PERIOD_ALPHA_LEVEL_{level_num}",
            "PERIOD_ALPHA_CONTRIBUTION",
        ]
    ]
    result["SUM_PERIOD_RETURN"] = result["SUM_PERIOD_RETURN"] / 100 + 1
    result["PERIOD_RETURN_CONTRIBUTION"] = (
        result["PERIOD_RETURN_CONTRIBUTION"] / 100 + 1
    )
    result[f"SUM_PERIOD_ALPHA_LEVEL_{level_num}"] = (
        result[f"SUM_PERIOD_ALPHA_LEVEL_{level_num}"] / 100 + 1
    )
    result["PERIOD_ALPHA_CONTRIBUTION"] = result["PERIOD_ALPHA_CONTRIBUTION"] / 100 + 1

    result = (
        result.groupby(["PORTFOLIO_NAME", "TICKER_SYMBOL", "SEC_SHORT_NAME"]).prod() - 1
    ) * 100
    result["PERIOD_RETURN_CONTRIBUTION_PCT"] = (
        100
        * result["PERIOD_RETURN_CONTRIBUTION"]
        / (result["PERIOD_RETURN_CONTRIBUTION"].sum())
    )
    result["PERIOD_ALPHA_CONTRIBUTION_PCT"] = (
        100
        * result["PERIOD_ALPHA_CONTRIBUTION"]
        / (result["PERIOD_ALPHA_CONTRIBUTION"].sum())
    )

    result = result.reset_index()
    result = result.merge(df_start, how="left")
    result = result.merge(df_end, how="left")
    col_rename = {
        "PORTFOLIO_NAME": "组合名称",
        "TICKER_SYMBOL": "基金代码",
        "SEC_SHORT_NAME": "基金名称",
        f"LEVEL_{level_num}": f"知己分类{level_num}级",
        "START_TRADE_DT": "开始日期",
        "START_WEIGHT": "开始权重",
        "SUM_PERIOD_RETURN": "区间累计收益",
        f"SUM_PERIOD_ALPHA_LEVEL_{level_num}": "区间超额收益",
        "PERIOD_RETURN_CONTRIBUTION": "区间累计收益贡献",
        "PERIOD_RETURN_CONTRIBUTION_PCT": "区间累计收益贡献占比",
        "PERIOD_ALPHA_CONTRIBUTION": "区间超额收益贡献",
        "PERIOD_ALPHA_CONTRIBUTION_PCT": "区间超额收益贡献占比",
        "END_TRADE_DT": "结束日期",
        "END_WEIGHT": "结束权重",
    }
    numeric_col = [
        "START_WEIGHT",
        "PERIOD_RETURN_CONTRIBUTION",
        "PERIOD_RETURN_CONTRIBUTION_PCT",
        "END_WEIGHT",
        "SUM_PERIOD_RETURN",
        f"SUM_PERIOD_ALPHA_LEVEL_{level_num}",
        "PERIOD_ALPHA_CONTRIBUTION",
        "PERIOD_ALPHA_CONTRIBUTION_PCT",
    ]
    cols = [key for key in col_rename.keys()]
    # percent_col = [
    #     "START_WEIGHT", "PERIOD_RETURN_CONTRIBUTION",
    #     "PERIOD_RETURN_CONTRIBUTION_PCT","END_WEIGHT"
    # ]
    # result[percent_col] = result[percent_col].applymap(
    #     lambda s: "" if np.isnan(s) else format(s, "0.4f")
    # )
    df_type = get_fund_own_type(level_num=level_num)
    result = result.merge(df_type, how="left")
    return_group = (
        result.groupby(["PORTFOLIO_NAME", f"LEVEL_{level_num}"])[
            [
                "START_WEIGHT",
                "PERIOD_RETURN_CONTRIBUTION",
                "PERIOD_RETURN_CONTRIBUTION_PCT",
                "PERIOD_ALPHA_CONTRIBUTION",
                "PERIOD_ALPHA_CONTRIBUTION_PCT",
                "END_WEIGHT",
            ]
        ]
        .sum()
        .reset_index()
    )
    return_sum = (
        result.groupby(["PORTFOLIO_NAME"])[
            [
                "START_WEIGHT",
                "PERIOD_RETURN_CONTRIBUTION",
                "PERIOD_RETURN_CONTRIBUTION_PCT",
                "PERIOD_ALPHA_CONTRIBUTION",
                "PERIOD_ALPHA_CONTRIBUTION_PCT",
                "END_WEIGHT",
            ]
        ]
        .sum()
        .reset_index()
    )

    for col in numeric_col:
        if col in return_sum.columns:
            return_sum[col] = return_sum[col].apply(
                lambda s: "" if np.isnan(s) else format(s, "0.4f")
            )
        if col in return_group.columns:
            return_group[col] = return_group[col].apply(
                lambda s: "" if np.isnan(s) else format(s, "0.4f")
            )
        if col in result.columns:
            result[col] = result[col].apply(
                lambda s: "" if np.isnan(s) else format(s, "0.4f")
            )

    result = result[cols].rename(columns=col_rename)
    return_group.rename(columns=col_rename, inplace=True)
    return_sum.rename(columns=col_rename, inplace=True)
    return result, return_group, return_sum


def get_portfolios_products_contribution(
    portfolio_list: list = None,
    start_date: str = None,
    end_date: str = None,
    level_num: int = 1,
):
    abs_ret_list = []
    alpha_list = []
    sum_list = []
    if portfolio_list is None:
        portfolio_df = dm.get_portfolio_info()
        portfolio_df = portfolio_df.query("IF_LISTED == 1")
        portfolio_df = portfolio_df[pd.isnull(portfolio_df["DELISTED_DATE"])]
        portfolio_list = portfolio_df["PORTFOLIO_NAME"].tolist()
    for port_name in portfolio_list:
        df1, df2, df3 = cal_periods_prodcuts_contribution(
            port_name, start_date, end_date, level_num=level_num
        )
        abs_ret_list.append(df1)
        alpha_list.append(df2)
        sum_list.append(df3)

    abs_ret = pd.concat(abs_ret_list)
    alpha_ret = pd.concat(alpha_list)
    sum_ret = pd.concat(sum_list)
    return abs_ret, alpha_ret, sum_ret


if __name__ == "__main__":
    abs_ret_list = []
    alpha_list = []
    sum_list = []
    start_date = "20240101"
    end_date = "20241202"
    level_num = 2
    file_path = f"""f:/BaiduNetdiskWorkspace/1-基金投研/2.1-监控/2-定时数据/绩效分析报告/绩效分析{start_date}-{end_date}.xlsx"""
    port_list = [
        "知己优选-短债增强",
        "知己优选-月月享",
    ]
    abs_ret, alpha_ret, sum_ret = get_portfolios_products_contribution(
        start_date=start_date,
        end_date=end_date,
        portfolio_list=port_list,
        level_num=level_num,
    )

    with pd.ExcelWriter(file_path) as writer:
        abs_ret.to_excel(writer, sheet_name="累计收益", index=False)
        alpha_ret.to_excel(writer, sheet_name="超额收益", index=False)
        sum_ret.to_excel(writer, sheet_name="总收益", index=False)
