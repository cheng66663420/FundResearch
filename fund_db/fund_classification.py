# -*- coding: utf-8 -*-
"""
Created on Fri May 27 15:32:52 2022

@author: Wilcoxon
"""


import pandas as pd

import quant_utils.data_moudle as dm
from quant_utils.db_conn import DB_CONN_JJTG_DATA
from quant_utils.utils import get_report_date


def get_type_wind_own():
    """
    获取wind分类与重分类的对照表

    Returns
    -------
    DataFrame
        DESCRIPTION.

    """
    query_sql = """
     SELECT *
     from fund_type_wind_map
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


def get_fund_type_jy_map():
    return DB_CONN_JJTG_DATA.exec_query("SELECT * from fund_type_jy_map")


class FundClassifition:
    def __init__(self, date, period_num) -> None:
        self.date = date
        self.period_num = period_num
        self.report_date = get_report_date(self.date, self.period_num)[-1]
        self.fund_type = self._prepare_data()

    def _prepare_data(self) -> None:
        """
        预处理数据,获取wind分类
        """
        # 获取wind分类
        fund_type = dm.get_jy_fund_type(self.report_date)
        fund_type["REPORT_DATE"] = self.report_date
        fund_type["PUBLISH_DATE"] = dm.offset_period_dt(self.report_date, 1, "m")
        # 与自己分类进行对应
        fund_type_map = get_fund_type_jy_map()
        fund_type = fund_type.merge(
            fund_type_map,
            on=["INDUSTRIESNAME_1", "INDUSTRIESNAME_2", "INDUSTRIESNAME_3"],
        )
        fund_info = dm.get_fund_info(self.report_date)
        fund_type = fund_type.merge(fund_info, on=["TICKER_SYMBOL"])

        # 获取最新分类，并进行数据处理
        fund_type = fund_type.drop(
            columns=[
                "ID",
                "S_INFO_SECTORENTRYDT",
                "S_INFO_SECTOREXITDT",
            ]
        )

        # 计算滚动period_num期风险暴露
        risk_exposure = dm.get_risk_exposure_period_mean(self.date, self.period_num)

        # 计算滚动period_num期港股暴露
        hk_exposure = dm.get_hk_exposure_period_mean(self.date, self.period_num)

        # 计算滚动period_num期信用债与利率债持仓占比
        bond_holdings = dm.get_fund_bond_holdings_period_mean(
            self.date, self.period_num
        )

        # 计算滚动period_num期赛道标签
        sector_exposure = dm.get_sector_exposure_period_mean(
            self.date, self.period_num
        ).query("SECTOR_RATIO_IN_NA >50")

        # 合并标签
        fund_type = (
            fund_type.merge(risk_exposure, on=["TICKER_SYMBOL"])
            .merge(bond_holdings, on=["TICKER_SYMBOL"], how="left")
            .merge(hk_exposure, on=["TICKER_SYMBOL"], how="left")
            .merge(sector_exposure, on=["TICKER_SYMBOL"], how="left")
        )

        fund_type["LEVEL_3"] = None
        return fund_type

    def get_level_1(self, fund_type):
        if fund_type["LEVEL_1"] not in ["细分", "固收+", "固收"]:
            return fund_type["LEVEL_1"]

        if fund_type["EQUITY_RATIO_IN_NA"] > 60:
            return "主动权益"

        elif fund_type["EQUITY_RATIO_IN_NA"] > 40:
            return "平衡型"

        elif fund_type["RISK_EXPOSURE"] > 0.5:
            return "固收+"
        elif fund_type["BOND_RATIO_IN_NA"] > 50:
            return "固收"
        else:
            return fund_type["LEVEL_1"]

    def get_level_2(self, fund_type):
        if fund_type["LEVEL_1"] == "固收":
            if fund_type["LEVEL_2"] == "短债" or "短" in fund_type["SEC_SHORT_NAME"]:
                return "短债"
            else:
                return "中长债"
        elif fund_type["LEVEL_1"] == "平衡型":
            return "平衡型"

        elif fund_type["LEVEL_1"] == "固收+":
            if fund_type["COVERTBOND_RATIO_IN_NA"] > 50:
                return "可转债"
            elif fund_type["RISK_EXPOSURE"] > 35:
                return "(35+)"
            elif fund_type["RISK_EXPOSURE"] > 25:
                return "(25-35)"
            elif fund_type["RISK_EXPOSURE"] > 15:
                return "(15-25)"
            elif fund_type["RISK_EXPOSURE"] > 5:
                return "(05-15)"
            else:
                return "(00-05)"

        elif fund_type["LEVEL_1"] == "主动权益":
            if fund_type["LEVEL_2"] == "指数增强":
                return "指数增强"

            else:
                return (
                    "港股主题"
                    if fund_type["F_PRT_HKSTOCKVALUETONAV"] > 50
                    else "A股主题"
                )

        elif fund_type["LEVEL_1"] in ["权益指数", "债券指数", "商品", "货币", "QDII"]:
            if (
                "ETF" in fund_type["SEC_SHORT_NAME"]
                and "联接" in fund_type["SEC_SHORT_NAME"]
            ):
                return "ETF联接"
            elif "ETF" in fund_type["SEC_SHORT_NAME"]:
                return "ETF"
            elif "存单" in fund_type["SEC_SHORT_NAME"]:
                return "存单基金"
            elif "QDII" in fund_type["SEC_SHORT_NAME"]:
                return fund_type["LEVEL_2"]
            else:
                return fund_type["LEVEL_1"] + "基金"

        else:
            return fund_type["LEVEL_2"]

    def get_level_3(self, fund_type):
        # 固收类
        if fund_type["LEVEL_1"] == "固收":
            if fund_type["LEVEL_2"] == "短债":
                if (
                    "中短" in fund_type["SEC_SHORT_NAME"]
                    or fund_type["INDUSTRIESNAME_3"] == "中短期纯债型"
                ):
                    return "中短债"
                elif "超短" in fund_type["SEC_SHORT_NAME"]:
                    return "超短债"
                return "短债"

            if fund_type["LEVEL_2"] == "中长债":
                if fund_type["CREDIT_TO_INTEREST"] > 2:
                    return "信用债"
                elif fund_type["CREDIT_TO_INTEREST"] < 0.5:
                    return "利率债"
                else:
                    return "券种均衡"

        # 权益类
        if fund_type["LEVEL_1"] == "主动权益":
            if fund_type["LEVEL_2"] in ["A股主题", "港股主题"]:
                return (
                    "无主题" if pd.isnull(fund_type["SECTOR"]) else fund_type["SECTOR"]
                )

            elif fund_type["LEVEL_2"] == "指数增强":
                return dm.get_fund_tracking_idx(
                    fund_type["TICKER_SYMBOL"], fund_type["REPORT_DATE"]
                )

        # 固收+
        if fund_type["LEVEL_1"] == "固收+":
            if fund_type["EQUITY_TO_COVERTBOND"] > 2:
                return "权益增强"
            elif fund_type["EQUITY_TO_COVERTBOND"] < 0.5:
                return "转债增强"
            else:
                return "均衡增强"

        # 被动指数基金
        if fund_type["LEVEL_2"] in [
            "ETF联接",
            "ETF",
            "存单基金",
            "商品基金",
            "国际(QDII)增强指数型股票基金",
            "国际(QDII)被动指数型股票基金",
            "权益指数基金",
        ]:
            return dm.get_fund_tracking_idx(
                fund_type["TICKER_SYMBOL"], fund_type["REPORT_DATE"]
            )

    def cal_fund_type_own(self):
        self.fund_type["LEVEL_1"] = self.fund_type.apply(
            lambda s: self.get_level_1(s), axis=1
        )
        self.fund_type["LEVEL_2"] = self.fund_type.apply(
            lambda s: self.get_level_2(s), axis=1
        )
        self.fund_type["LEVEL_3"] = self.fund_type.apply(
            lambda s: self.get_level_3(s), axis=1
        )
        self.fund_type.drop(
            columns=["INDUSTRIESNAME_1", "INDUSTRIESNAME_2", "INDUSTRIESNAME_3"],
            inplace=True,
        )

    def main(self):
        self.cal_fund_type_own()
        DB_CONN_JJTG_DATA.upsert(self.fund_type, table="fund_type_own")
        # print(self.fund_type)
        query = f"""
    		SELECT
            PUBLISH_DATE,
			REPORT_DATE,
			TICKER_SYMBOL,
			LEVEL_1,
			CONCAT( LEVEL_1, "-", LEVEL_2 ) AS LEVEL_2,
		CASE
				LEVEL_3 IS NULL 
				WHEN 1 THEN
				CONCAT( LEVEL_1, "-", LEVEL_2 ) 
                ELSE CONCAT( LEVEL_1, "-", LEVEL_2, "-", LEVEL_3 ) 
			END AS LEVEL_3 
		FROM
			fund_type_own 
		WHERE
			REPORT_DATE = {self.report_date}
        """
        df = DB_CONN_JJTG_DATA.exec_query(query)
        # print(df)
        DB_CONN_JJTG_DATA.upsert(df, table="fund_type_own_temp")


if __name__ == "__main__":
    month = ["0228", "0531", "0831", "1130"]
    dates_list = [str(i) + j for i in range(2024, 2025) for j in month]
    # # dates_list += ["20230131"]
    for end_date in dates_list:
        test = FundClassifition(end_date, 4)
        test.main()
        print(f"{end_date}写入成功")
        print("==" * 35)
    # test = FundClassifition("20240430", 4)
    # test.main()
    # df = test.fund_type
    # print(df)
