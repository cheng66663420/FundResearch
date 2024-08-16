import numpy as np
import pandas as pd
from dateutil.parser import parse

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA


def get_temp_portfolio_holding() -> pd.DataFrame:
    """
    获取预计调仓的组合数据

    Returns
    -------
    pd.DataFrame
        columns = [
            TRADE_DT,
            PORTFOLIO_NAME,
            TICKER_SYMBOL,
            WEIGHT,
            ALTERNATIVE_TICKER_SYMBOL,
            MANAGEMENT_COMPANY_NAME
        ]
    """

    query_sql = """
    SELECT
        a.TRADE_DT,
        a.PORTFOLIO_NAME,
        a.TICKER_SYMBOL,
        a.WEIGHT,
        a.ALTERNATIVE_TICKER_SYMBOL,
        b.MANAGEMENT_COMPANY_NAME 
    FROM
        temp_portfolio_products_weights a
        JOIN fund_info b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
    WHERE
        1 = 1 
        AND b.EXPIRE_DATE IS NULL
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_holding_new(portfolio_name: str = None) -> pd.DataFrame:
    """
    获取组合最新持仓数据

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None

    Returns
    -------
    pd.DataFrame
        columns = [
            TRADE_DT,
            PORTFOLIO_NAME,
            TICKER_SYMBOL,
            WEIGHT,
            MANAGEMENT_COMPANY_NAME
        ]
    """
    query_sql = """
        SELECT
            a.TRADE_DT,
            a.PORTFOLIO_NAME,
            a.TICKER_SYMBOL,
            a.WEIGHT,
            a.ALTERNATIVE_TICKER_SYMBOL,
            b.MANAGEMENT_COMPANY_NAME 
        FROM
            view_portfolio_holding_new a
            JOIN fund_info b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL
        WHERE
            1 = 1 
            AND b.EXPIRE_DATE IS NULL
    """
    if portfolio_name is not None:
        query_sql += f" AND a.PORTFOLIO_NAME = '{portfolio_name}'"
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_jjtg_tranche() -> pd.DataFrame:
    """
    获取基金投顾备选池

    Returns
    -------
    pd.DataFrame
        columns = [
            TICKER_SYMBOL,
            SEC_SHORT_NAME,
            IF_IN_TRANCHE,
            TRANCHE,
            TA,
            MEDIAN
        ]
    """
    query_sql = """
    SELECT
        a.TICKER_SYMBOL,
        c.SEC_SHORT_NAME,
        a.IF_IN_TRANCHE,
        a.TRANCHE,
        b.TA,
        d.MEDIAN,
        a.FIRST_BUY
    FROM
        portfolio_basic_products a
        JOIN fund_info c ON a.TICKER_SYMBOL = c.TICKER_SYMBOL
        LEFT JOIN XY_BOP b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL
        JOIN risk_level d ON b.RISK_LEVEL = d.RISK_LEVEL 
    WHERE
        1 = 1 
        AND c.EXPIRE_DATE IS NULL 
        AND a.IF_IN_TRANCHE = 1 
        AND b.TA IS NOT NULL
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_risk_level(risk_score: float) -> str:
    """
    获取风险等级

    Parameters
    ----------
    risk_score : float
        风险得分

    Returns
    -------
    str
        风险等级
    """
    query_sql = f"""
    SELECT
        RISK_LEVEL 
    FROM
        risk_level 
    WHERE
        1 = 1 
        AND {risk_score} BETWEEN min 
        AND MAX
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)["RISK_LEVEL"].values[0]


def get_fund_asset_type() -> pd.DataFrame:
    """
    获取基金对应的类别

    Returns
    -------
    pd.DataFrame
        columns=[
            TICKER_SYMBOL,
            LEVEL_SUM,
            ASSET_TYPE
        ]
    """
    query_sql = """
    SELECT
        a.TICKER_SYMBOL,
        b.LEVEL_SUM,
        b.ASSET_TYPE 
    FROM
        fund_type_own a
        JOIN fund_type_sum b ON a.LEVEL_1 = b.LEVEL_1 
        AND a.LEVEL_2 = b.LEVEL_2 
    WHERE
        1 = 1 
        AND a.REPORT_DATE = (
        SELECT
            max( REPORT_DATE ) 
        FROM
            fund_type_own 
        WHERE
        REPORT_DATE <= CURRENT_DATE () 
        ) 
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_portfolio_constraint(portfolio_name: str) -> pd.DataFrame:
    """
    获取组合约束条件

    Parameters
    ----------
    portfolio_name : str
        组合名称

    Returns
    -------
    pd.DataFrame
        组合在固收、权益、商品及货币上的约束条件

    """
    query = f"""
    SELECT
        *
    from 
        portfolio_constraint
    where
        1=1
        and PORTFOLIO_NAME = '{portfolio_name}'
    """
    df = DB_CONN_JJTG_DATA.exec_query(query)
    if df.empty:
        raise ValueError(f"{portfolio_name} 组合没有约束条件，请检查！")
    df.drop(columns=["ID", "UPDATE_TIME"], inplace=True)
    df.set_index(["PORTFOLIO_NAME"], inplace=True)
    df = (
        df.unstack().reset_index().rename(columns={"level_0": "ASSET_TYPE", 0: "VALUE"})
    )
    df[["ASSET_TYPE", "TYPE"]] = df["ASSET_TYPE"].str.split("_", expand=True)
    df = df.pivot_table(
        index="ASSET_TYPE", columns="TYPE", values="VALUE"
    ).reset_index()
    df["ASSET_TYPE"] = df["ASSET_TYPE"].map(
        {"BOND": "固收", "STOCK": "权益", "OTHER": "商品", "CASH": "货币"}
    )
    return df


def get_portfolio_turnover(portfolio_name: str = None) -> pd.DataFrame:
    """
    获取组合换手率

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None

    Returns
    -------
    pd.DataFrame
        columns = [
            TRADE_DT,
            PORTFOLIO_NAME,
            TURNOVER
        ]
    """
    where_sql = ""
    if portfolio_name is not None:
        where_sql = f"and c.portfolio_name = '{portfolio_name}'"
    query_sql = f"""
    WITH a AS (
        SELECT DISTINCT
            a.TRADE_DT,
            a.PORTFOLIO_NAME,
            b.PREV_TRADE_DATE 
        FROM
            portfolio_products_weights a
            JOIN md_tradingdaynew b ON a.TRADE_DT = b.TRADE_DT
            JOIN portfolio_info c ON c.PORTFOLIO_NAME = a.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND b.IF_TRADING_DAY = 1 
            AND b.SECU_MARKET = 83 
            AND a.TRADE_DT > c.LISTED_DATE 
            AND c.IF_LISTED = 1 
            {where_sql}
        ),
        hold AS (
        SELECT
            a.TRADE_DT,
            a.PREV_TRADE_DATE,
            pw.PORTFOLIO_NAME,
            pw.TICKER_SYMBOL,
            pw.START_WEIGHT,
            pw.END_WEIGHT 
        FROM
            portfolio_derivatives_products_weights pw
            JOIN a ON a.TRADE_DT = pw.TRADE_DT 
            AND a.PORTFOLIO_NAME = pw.PORTFOLIO_NAME 
        ),
        pre_hold AS (
        SELECT
            pw.TRADE_DT,
            a.TRADE_DT AS NEXT_TRADE_DT,
            pw.PORTFOLIO_NAME,
            pw.TICKER_SYMBOL,
            pw.START_WEIGHT,
            pw.END_WEIGHT 
        FROM
            portfolio_derivatives_products_weights pw
            JOIN a ON a.PREV_TRADE_DATE = pw.TRADE_DT 
            AND a.PORTFOLIO_NAME = pw.PORTFOLIO_NAME 
        ),
        d AS (
        SELECT
            hold.TRADE_DT,
            hold.PREV_TRADE_DATE,
            hold.TICKER_SYMBOL,
            hold.PORTFOLIO_NAME,
            ifnull( hold.START_WEIGHT, 0 ) AS NEW_WEIGHT,
            ifnull( pre_hold.END_WEIGHT, 0 ) AS OLD_WEIGHT 
        FROM
            hold
            LEFT JOIN pre_hold ON hold.TICKER_SYMBOL = pre_hold.TICKER_SYMBOL 
            AND hold.PREV_TRADE_DATE = pre_hold.TRADE_DT 
            AND hold.PORTFOLIO_NAME = pre_hold.PORTFOLIO_NAME UNION
        SELECT
            pre_hold.NEXT_TRADE_DT,
            pre_hold.trade_dt,
            pre_hold.TICKER_SYMBOL,
            pre_hold.PORTFOLIO_NAME,
            ifnull( hold.START_WEIGHT, 0 ),
            ifnull( pre_hold.END_WEIGHT, 0 ) 
        FROM
            hold
            RIGHT JOIN pre_hold ON hold.TICKER_SYMBOL = pre_hold.TICKER_SYMBOL 
            AND hold.PREV_TRADE_DATE = pre_hold.TRADE_DT 
            AND hold.PORTFOLIO_NAME = pre_hold.PORTFOLIO_NAME 
        ORDER BY
            TRADE_DT,
            ticker_symbol 
        ) SELECT
        DATE_FORMAT( d.TRADE_DT, '%Y%m%d' ) AS TRADE_DT,
        d.PORTFOLIO_NAME,
        sum(
        abs ( d.NEW_WEIGHT - d.OLD_WEIGHT ))/ 2 AS TURNOVER 
    FROM
        d 
    WHERE
        1 = 1 
    GROUP BY
        d.TRADE_DT,
        d.PORTFOLIO_NAME
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_temp_portfolio_turnover(portfolio_name: str = None) -> pd.DataFrame:
    """
    获取预计调仓组合的换手率

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None

    Returns
    -------
    pd.DataFrame
        columns = [PORTFOLIO_NAME, TURNOVER]
    """
    where_sql = ""
    if portfolio_name is not None:
        where_sql = f"and PORTFOLIO_NAME = '{portfolio_name}'"
    query_sql = f"""
    WITH a AS (
        SELECT
            a.PORTFOLIO_NAME,
            a.TICKER_SYMBOL,
            ifnull( a.WEIGHT, 0 ) AS NEW_WEIGHT,
            ifnull( b.WEIGHT, 0 ) AS OLD_WEIGHT 
        FROM
            temp_portfolio_products_weights a
            JOIN portfolio_info c ON c.PORTFOLIO_NAME = a.PORTFOLIO_NAME
            LEFT JOIN view_portfolio_holding_new b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
            AND a.TICKER_SYMBOL = b.TICKER_SYMBOL 
        WHERE
            1 = 1 
        
        AND a.trade_dt > c.LISTED_DATE UNION
        SELECT
            b.PORTFOLIO_NAME,
            b.TICKER_SYMBOL,
            ifnull( a.WEIGHT, 0 ) AS NEW_WEIGHT,
            ifnull( b.WEIGHT, 0 ) AS OLD_WEIGHT 
        FROM
            temp_portfolio_products_weights a
            RIGHT JOIN view_portfolio_holding_new b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
            AND a.TICKER_SYMBOL = b.TICKER_SYMBOL
            JOIN portfolio_info c ON c.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND b.trade_dt > c.LISTED_DATE 
        ) SELECT
        PORTFOLIO_NAME,
        sum(
        abs( new_weight - old_weight ))/ 2 AS TURNOVER 
    FROM
        a 
    WHERE
        1 = 1 
        AND PORTFOLIO_NAME IN ( SELECT PORTFOLIO_NAME FROM temp_portfolio_products_weights ) 
        {where_sql}
    GROUP BY
        PORTFOLIO_NAME
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def check_weight_sum(df: pd.DataFrame) -> None:
    """
    检查组合权重和是否为100

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓组合及权重

    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    result = df.groupby("PORTFOLIO_NAME")["WEIGHT"].sum()
    result = result.reset_index()
    for _, val in result.iterrows():
        if round(val["WEIGHT"] - 100, 4) != 0:
            raise ValueError(f"{val['PORTFOLIO_NAME'] } 组合权重之和不为100，请检查!")
        else:
            print(f"{val['PORTFOLIO_NAME']}校验通过:权重加总为100")


def check_fund_in_trache(df: pd.DataFrame) -> None:
    """
    检查基金及替补基金是否在基金投顾备选内,权重是否超越所属池子

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓组合及权重
    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    weight_dict = {
        "优选池": 10,
        "核心池": 20,
    }
    jjtg_tranche = get_jjtg_tranche()
    jjtg_tranche = dict(zip(jjtg_tranche["TICKER_SYMBOL"], jjtg_tranche["TRANCHE"]))
    # df = df.merge(jjtg_tranche, on="TICKER_SYMBOL", how="left")
    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        for _, val in portfolio_holding.iterrows():
            if val["TICKER_SYMBOL"] is None:
                continue
            if val["TICKER_SYMBOL"] not in jjtg_tranche.keys():
                raise ValueError(
                    f"""{val["PORTFOLIO_NAME"]}-主基金{val['TICKER_SYMBOL']}"""
                    + """不在基金投顾产品池内，请检查!"""
                )

            if val["WEIGHT"] > weight_dict[jjtg_tranche[val["TICKER_SYMBOL"]]]:
                raise ValueError(
                    f"""{val["PORTFOLIO_NAME"]}-主基金{val['TICKER_SYMBOL']}权重{val["WEIGHT"]:.2f}%,"""
                    + f"""超过{weight_dict[jjtg_tranche[val["TICKER_SYMBOL"]]]:.2f}%，请检查！"""
                )
            if "ALTERNATIVE_TICKER_SYMBOL" not in val.keys():
                continue
            if val["ALTERNATIVE_TICKER_SYMBOL"] is None:
                continue
            if val["ALTERNATIVE_TICKER_SYMBOL"] not in jjtg_tranche.keys():
                raise ValueError(
                    f"""{val["PORTFOLIO_NAME"]}-{val['TICKER_SYMBOL']}的备选基金{val['ALTERNATIVE_TICKER_SYMBOL']}"""
                    + """不在基金投顾产品池内，请检查!"""
                )
            if (
                val["WEIGHT"]
                > weight_dict[jjtg_tranche[val["ALTERNATIVE_TICKER_SYMBOL"]]]
            ):
                raise ValueError(
                    f"""{val["PORTFOLIO_NAME"]}-{val['TICKER_SYMBOL']}的备选基金{val['ALTERNATIVE_TICKER_SYMBOL']}"""
                    + f"""权重超过{weight_dict[jjtg_tranche[val["ALTERNATIVE_TICKER_SYMBOL"]]]:.2f}%，请检查！"""
                )
        print(f"{portfolio_name}校验通过:主基金及备选基金均在备选池内,比例符合要求")


def check_realted_fund(df: pd.DataFrame) -> None:
    """
    检查关联方基金占比是否超过40%

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓组合及权重
    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        df_group = portfolio_holding.groupby("MANAGEMENT_COMPANY_NAME")["WEIGHT"].sum()
        df_group = df_group.reset_index()
        for _, val in df_group.iterrows():
            if val["WEIGHT"] > 20:
                print(
                    f"{portfolio_name}-{val['MANAGEMENT_COMPANY_NAME']}占比{val['WEIGHT']:.2f},权重超20%"
                )
        related_company = df_group[
            df_group["MANAGEMENT_COMPANY_NAME"].isin(["兴证全球基金", "南方基金"])
        ]
        if related_company["WEIGHT"].sum() > 40:
            raise ValueError(f"{portfolio_name} 关联方基金超过40%")
        else:
            print(f"{portfolio_name}校验通过:关联方基金没有超过40%内,比例符合要求")


def check_risk_level(df: pd.DataFrame) -> None:
    """
    检查组合风险等级是否复核约束

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓组合及权重
    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    jjtg_tranche = get_jjtg_tranche()
    portfolio_info = dm.get_portfolio_info()
    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        temp = portfolio_holding.merge(jjtg_tranche, on="TICKER_SYMBOL", how="left")
        risk_score = np.sum(temp["WEIGHT"] * temp["MEDIAN"]) / 100
        risk_level = get_risk_level(risk_score)
        portfolio_risk_level = portfolio_info.query("PORTFOLIO_NAME==@portfolio_name")[
            "RISK_LEVEL"
        ].values[0]
        if risk_level != portfolio_risk_level:
            raise ValueError(
                f"{portfolio_name}校验失败,预期{portfolio_risk_level},实际{risk_level}"
            )
        else:
            print(
                f"{portfolio_name}校验通过:风险等级复核说明书一致,风险等级为{risk_level}"
            )


def check_asset_constrain(df: pd.DataFrame) -> None:
    """
    检查组合资产配置是否复核约束

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓组合及权重
    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    fund_asset_type = get_fund_asset_type()
    df = df.merge(fund_asset_type, on="TICKER_SYMBOL", how="left")
    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        temp = portfolio_holding.groupby("ASSET_TYPE")["WEIGHT"].sum().reset_index()
        portfolio_constraint = get_portfolio_constraint(portfolio_name)
        temp = temp.merge(portfolio_constraint, on="ASSET_TYPE", how="left")
        temp["IF_CONSTRAIN"] = temp.apply(
            lambda s: (
                1
                if s["WEIGHT"] <= s["MAX"] + 0.01 and s["WEIGHT"] >= s["MIN"] - 0.01
                else 0
            ),
            axis=1,
        )
        if temp["IF_CONSTRAIN"].all() != 1:
            temp_df = temp.query("IF_CONSTRAIN !=1")
            temp_dict = dict(zip(temp_df["ASSET_TYPE"], temp_df["WEIGHT"]))
            str = [f"{key}: {value:.2f}%不符合要求" for key, value in temp_dict.items()]
            str = ",".join(str)
            raise ValueError(f"{portfolio_name} 资产配置校验失败,{str}")
        else:
            print(f"{portfolio_name}校验通过:资产配置符合说明书要求")


def check_portfolio_turnover(df: pd.DataFrame) -> None:
    """
    检查组合过去1年换手率是否超过200%

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓组合及权重
    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        end_date = portfolio_holding["TRADE_DT"].values[0].strftime("%Y%m%d")
        start_date = dm.offset_period_trade_dt(end_date, -1, "y")
        portfolio_turnover = get_period_portfolio_turnover(
            portfolio_name=portfolio_name, start_date=start_date, end_date=end_date
        )
        if portfolio_turnover.empty:
            turnover = 0
        else:
            turnover = portfolio_turnover["TURNOVER"].values[0]
        if turnover > 200:
            raise ValueError(f"{portfolio_name} 校验失败: 过去1年换手率{turnover:.2f}%")
        print(f"{portfolio_name}校验通过:过去1年换手率{turnover:.2f}%")


def check_first_buy_amount(df: pd.DataFrame) -> None:
    """
    检查组合首次买入金额是否小于等于系统值

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓组合及权重
    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    fund_trache_df = get_jjtg_tranche()

    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        temp = portfolio_holding.merge(fund_trache_df, on="TICKER_SYMBOL")
        temp["FIRST_BUY_AMOUNT"] = temp["FIRST_BUY"] / temp["WEIGHT"] * 100
        portfolio_first_buy_amount = (
            dm.get_portfolio_info()
            .query("PORTFOLIO_NAME==@portfolio_name")["FIRST_BUY"]
            .values[0]
        )
        if temp["FIRST_BUY_AMOUNT"].max() > portfolio_first_buy_amount:
            raise ValueError(
                f"{portfolio_name} 校验失败: 首次买入金额{temp['FIRST_BUY_AMOUNT'].max():.2f}大于{portfolio_first_buy_amount:.2f}"
            )
        print(
            f"{portfolio_name}校验通过:首次买入金额{temp['FIRST_BUY_AMOUNT'].max():.2f}"
        )


def check_portfolio_main_func(df: pd.DataFrame) -> pd.DataFrame:
    """
    组合校验的主函数

    Parameters
    ----------
    df : pd.DataFrame
        本期持仓

    Returns
    -------
    pd.DataFrame
        columns = ["校验项", "校验结果", "备注"]
    """
    if df.empty:
        raise ValueError("没有持仓数据，请检查！")
    df = df.copy()
    func_dict = {
        "校验组合权重之和": check_weight_sum,
        "校验基金是否在池及比例": check_fund_in_trache,
        "校验关联基金权重": check_realted_fund,
        "校验风险等级是否一致": check_risk_level,
        "校验资产配置是否符合说明书要求": check_asset_constrain,
        "检验过去1年换手率": check_portfolio_turnover,
        "校验起投金额": check_first_buy_amount,
    }
    result_dict = {}
    counter = 0
    for key, func in func_dict.items():
        result_dict[key] = {}
        try:
            func(df)
            result_dict[key]["校验结果"] = "通过"
            result_dict[key]["备注"] = None
            counter += 1
        except Exception as e:
            print(e)
            result_dict[key]["校验结果"] = "失败"
            result_dict[key]["备注"] = str(e)
    result = pd.DataFrame.from_dict(result_dict, orient="index")
    result.reset_index(inplace=True)
    result.rename(
        columns={
            "index": "校验项",
        },
        inplace=True,
    )
    print(f"校验完成，共校验{len(func_dict)}项,通过{counter}项")
    return result


def get_period_portfolio_turnover(
    portfolio_name: str = None, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """
    获取组合区间换手率

    Parameters
    ----------
    portfolio_name : str, optional
        组合名称, by default None
    start_date : str, optional
        开始日期, by default None
    end_date : str, optional
        结束日期, by default None

    Returns
    -------
    pd.DataFrame
        columns = ["PORTFOLIO_NAME", "TURNOVER"]
    """
    turnover = get_portfolio_turnover(portfolio_name)
    if start_date is None:
        start_date = "19900101"
    else:
        start_date = parse(start_date).strftime("%Y%m%d")
    if end_date is None:
        end_date = "20991231"
    else:
        end_date = parse(end_date).strftime("%Y%m%d")
    turnover = turnover.query(
        f"TRADE_DT >= '{start_date}' and TRADE_DT <= '{end_date}'",
    )[["PORTFOLIO_NAME", "TURNOVER"]]
    turnover = turnover.groupby(by="PORTFOLIO_NAME").sum().reset_index()
    return turnover


def check_temp_portfolio_change() -> pd.DataFrame:
    """
    校验预计调仓组合信息

    Returns
    -------
    pd.DataFrame
        columns = ["PORTFOLIO_NAME", "TURNOVER"]
    """
    df = get_temp_portfolio_holding()
    result_list = []
    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        print(portfolio_name)
        temp_turnover = get_temp_portfolio_turnover(portfolio_name)
        end_date = portfolio_holding["TRADE_DT"].values[0].strftime("%Y%m%d")
        start_date = dm.offset_trade_dt(end_date, 365)
        turnover_1y = get_period_portfolio_turnover(
            portfolio_name=portfolio_name,
            end_date=end_date,
            start_date=start_date,
        )
        turnover_1y = 0 if turnover_1y.empty else turnover_1y["TURNOVER"].values[0]
        temp_turnover = (
            0 if temp_turnover.empty else temp_turnover["TURNOVER"].values[0]
        )

        turnover = turnover_1y + temp_turnover

        temp = check_portfolio_main_func(portfolio_holding)
        temp.loc[temp["校验项"] == "检验过去1年换手率", "校验结果"] = (
            "通过" if turnover < 200 else "失败"
        )
        temp.loc[temp["校验项"] == "检验过去1年换手率", "备注"] = (
            f"过去1年换手{turnover_1y:.2f}%,本次换手率{temp_turnover:.2f}%, 总换手率{turnover:.2f}%"
        )
        temp["校验组合"] = portfolio_name
        result_list.append(temp)
        print("===" * 20)
    result = pd.concat(result_list)
    result.set_index(["校验组合", "校验项"], inplace=True)
    return result


def check_portfolio() -> pd.DataFrame:
    """
    校验组合

    Returns
    -------
    pd.DataFrame
        columns = ["PORTFOLIO_NAME", "TURNOVER"]
    """
    df = get_holding_new()
    result_list = []
    for portfolio_name, portfolio_holding in df.groupby("PORTFOLIO_NAME"):
        print(portfolio_name)
        temp = check_portfolio_main_func(portfolio_holding)
        temp["校验组合"] = portfolio_name
        result_list.append(temp)
        print("===" * 20)
    result = pd.concat(result_list)
    result.set_index(["校验组合", "校验项"], inplace=True)
    return result


if __name__ == "__main__":
    result = get_holding_new()
    check_portfolio_turnover(result)
    # print(result.query("校验结果 == '失败'"))
    # df = get_holding_new("知己优选-货币小福星")
    # check_fund_in_trache(df)
