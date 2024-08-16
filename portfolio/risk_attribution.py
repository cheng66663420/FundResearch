import numpy as np
import pandas as pd

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA

# rootPath = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
# sys.path.append(rootPath)


DB_CONN = DB_CONN_JJTG_DATA


def cal_cov_matrix(
    ticker_symbol_list: list, start_date: str, end_date: str
) -> pd.DataFrame:
    """
    根据列表计算基金协方差矩阵

    Parameters
    ----------
    ticker_symbol_list : list
        基金代码list
    start_date : str
        开始日期
    end_date : str
        结束日期

    Returns
    -------
    pd.DataFrame
        年化协方差矩阵
    """
    nav_df = dm.get_fund_adj_nav(
        ticker_symbol=ticker_symbol_list, start_date=start_date, end_date=end_date
    )
    ret_df = (
        nav_df.pivot_table(
            index="TRADE_DT", columns="TICKER_SYMBOL", values="ADJUST_NAV"
        )
        .pct_change()
        .dropna()
    )

    return ret_df.cov() * 252


def cal_portfolio_std(weights_seiries, cov_mattrix):
    return np.sqrt(weights_seiries.dot(cov_mattrix).dot(weights_seiries))


def cal_margin_risk_contribution(weights_array, cov_mattrix):
    weights_array = np.asarray(weights_array)
    cov_mattrix = np.asmatrix(cov_mattrix)
    port_std = cal_portfolio_std(weights_array, cov_mattrix)
    return (cov_mattrix.dot(weights_array)) / port_std


def cal_ACTR(weights_array, cov_mattrix):
    weights_array = np.asarray(weights_array)
    cov_mattrix = np.asmatrix(cov_mattrix)
    mctr = cal_margin_risk_contribution(weights_array, cov_mattrix)
    return np.multiply(weights_array, mctr)


if __name__ == "__main__":
    cov = cal_cov_matrix(["163415", "110011", "163406"], "20221231", "20230601")
    weights_df = pd.Series([0.4, 0.4, 0.2], index=["163415", "110011", "163406"])
    print(np.sqrt(weights_df.dot(cov).dot(weights_df)))
    print(cal_portfolio_std(weights_df, cov))
