# -*- coding: utf-8 -*-
"""
Created on Thu Jun 16 21:51:11 2022

@author: Wilcoxon
"""
import datetime
import json
import os
import sys
import time

import numpy as np
import pandas as pd
import psutil
import pythoncom
import scipy.optimize as sco
import xlwings as xw
from dateutil.parser import parse

from PIL import ImageGrab


def load_json(file_path: str, encoding: str = "utf-8"):
    """
    加载json文件

    Parameters
    ----------
    file_path : str
        文件路径
    encoding : str, optional
        编码格式. The default is "utf-8".

    Returns
    -------
    dict
        json内容
    """
    with open(file_path, "r", encoding=encoding) as f:
        return json.load(f)


def kill_processes_containing(keyword: str):
    """
       杀死包含关键字的进程

    Parameters
    ----------
    keyword : str
        关键字
    """
    for proc in psutil.process_iter(["pid", "name"]):
        if keyword in proc.info["name"]:
            try:
                p = psutil.Process(proc.info["pid"])
                p.kill()  # 或者使用 p.terminate() 来更温和地尝试结束进程
                print(f"Killed process {proc.info['name']} with PID {proc.info['pid']}")
            except (psutil.NoSuchProcess, PermissionError) as e:
                print(
                    f"Failed to kill process {proc.info['name']} with PID {proc.info['pid']}: {e}"
                )


def save_excel_into_img(file_path: str, img_name: str, sheet_name: str = None):
    """
    将excel内容转换为图片

    Parameters
    ----------
    file_path : str
        excel文件路径
    sheetname : str
        表名
    img_name : str
        图片路径
    """
    os.system("taskkill /IM EXCEL.exe /F")
    pythoncom.CoInitialize()
    try:
        # 使用xlwings的app启动
        app = xw.App(
            visible=False,
            add_book=False,
        )
        app.display_alerts = False
        app.screen_updating = False
        # 打开文件
        file_path = os.path.abspath(file_path)
        wb = app.books.open(file_path)
        # 选定sheet
        if sheet_name is not None:
            if sheet_name in wb.sheet_names:
                sheet = wb.sheets[sheet_name]
            else:
                raise ValueError(f"sheet {sheet_name} not found")
        else:
            sheet = wb.sheets(wb.sheet_names[0])

    except Exception as e:
        pythoncom.CoUninitialize()
        raise e

    try:
        # 获取有内容的区域
        all = sheet.used_range
        # 复制图片区域
        all.api.CopyPicture()
        # 粘贴
        sheet.api.Paste()
        # 当前图片
        pic = sheet.pictures[-1]
        # 复制图片
        pic.api.Copy()
        time.sleep(3)  # 延迟一下操作，不然获取不到图片
        # 获取剪贴板的图片数据
        img = ImageGrab.grabclipboard()
        # 保存图片
        img.save(img_name)
        # 删除sheet上的图片
        pic.delete()

    except Exception as e:
        print(e)

    finally:
        # 关闭excel
        wb.close()
        # 退出xlwings的app启动
        app.quit()
        pythoncom.CoUninitialize()  # 关闭多线程
        # os.remove(file_path)


def get_report_date(date: str, num_period: int) -> list:
    """
    寻找日期前的n个报告期,不含当前季末

    Parameters
    ----------
    date : str
        日期.
    num_period : int
        需要寻找的n个报告期.

    Returns
    -------
    list
        报告期list.

    """
    # 将字符串解析为datetime
    date_time = parse(date)
    # 结果list
    date_list = []
    # 往前推num期
    for _ in range(num_period):
        temp_date = None
        # 时间减3个月
        temp_month = date_time.month - 3
        # 寻找对应的季末
        if temp_month > 9:
            temp_date = date_time.replace(month=12, day=31)

        elif temp_month > 6:
            temp_date = date_time.replace(month=9, day=30)

        elif temp_month > 3:
            temp_date = date_time.replace(month=6, day=30)

        elif temp_month > 0:
            temp_date = date_time.replace(month=3, day=31)

        else:
            temp_date = date_time.replace(month=12, day=31, year=date_time.year - 1)

        date_time = temp_date
        date_list.append(temp_date.strftime("%Y%m%d"))
    # 逆序
    date_list.reverse()
    return date_list


def to_wind_fund_code(code, if_suffix=True):
    """
    将代码转换成wind代码

    Parameters
    ----------
    code : int or str
        数字或者字符串的代码.
    if_suffix:
        是否带尾缀
    Returns
    -------
    符合wind要求的代码.
    """

    if if_suffix:
        return str(code).rjust(6, "0") + ".OF"
    else:
        return str(code).rjust(6, "0")


def display_time(content=""):
    """
    计时装饰器，计算函数的运行时长
    Args:
        content:

    Returns:
    """

    def wrapper(func):
        def time_wrapper(*args, **kwargs):
            print(content)
            t1 = time.time()
            result = func(*args, **kwargs)
            t2 = time.time()
            print(func.__name__ + "耗时:{:.4f} s".format(t2 - t1))
            print("==" * 20)
            return result

        return time_wrapper

    return wrapper


def cal_exposure(
    x: pd.DataFrame, y: pd.DataFrame, constraints: list[tuple], bounds: list[tuple]
) -> pd.DataFrame:
    """
    计算风险暴露

    Parameters
    ----------
    x : pd.DataFrame
        自变量
    y : pd.DataFrame
        因变量
    constraints : list[tuple]
        约束
    bounds : list[tuple]
        取值范围

    Returns
    -------
    pd.DataFrame
        _description_
    """

    # 获取因子收益列名
    industry_columns = x.columns
    # 保留日期
    date = x.index[-1]
    date = date if isinstance(date, str) else date.strftime("%Y-%m-%d")
    # 将X进行复制
    x = x.copy()
    # 增加一列常数项
    x["constant"] = 1

    # 转换为矩阵
    x_matrix = np.asmatrix(np.array(x))
    y_matrix = np.asmatrix(np.array(y))

    def fun_linear(A):
        # 将A转换为矩阵，并转置，结果为列向量
        A_temp = np.matrix(A).T
        n = len(A)
        weights = [np.exp(i / n) for i in range(1, n + 1)]

        ols = 0.5 * np.sum(
            np.multiply(weights, (x_matrix * A_temp - y_matrix)).T
            * (x_matrix * A_temp - y_matrix)
        )
        # 返回最小二乘法的优化函数
        return ols

    # 起始点
    initial_num = np.ones(x_matrix.shape[1]) / x_matrix.shape[1]

    # 优化器
    res = sco.minimize(
        fun_linear, initial_num, method="SLSQP", constraints=constraints, bounds=bounds
    )
    # 截距项
    alpha = res.x[-1]

    # 保存实际结果
    model_performance = pd.DataFrame(y_matrix, columns=["基金超额收益率"])

    # 计算模型拟合超额收益率
    model_performance["模型拟合超额收益率"] = np.array(
        np.matrix(x_matrix) * np.matrix(res.x).T
    )

    # 拟合与实际的相关性
    corr = np.corrcoef(
        model_performance["基金超额收益率"], model_performance["模型拟合超额收益率"]
    )[0, 1]

    # 计算残差波动率
    error_std = (
        np.std(
            model_performance["基金超额收益率"]
            - model_performance["模型拟合超额收益率"]
        )
        * np.sqrt(252)
        * 100
    )

    # 行业因子暴露，最后一个为截距项不保留
    beta = pd.DataFrame(
        res.x[:-1].tolist() + [corr**2, error_std, alpha],
        columns=[date],
        index=industry_columns.tolist() + ["r_squared", "residual_vol", "alpha"],
    ).T

    corr_df = pd.DataFrame(
        [date, corr**2, error_std, alpha],
        index=["TRADE_DT", "r_squared", "residual_vol", "alpha"],
    ).T

    return beta, corr_df


def yield_split_list(old_list: list, n_len: int):
    """
    通过yield将list拆分成长度为n的字符串

    Parameters
    ----------
    old_list : list
        老的list
    n_len : int
        拆分后的list长度

    Yields
    ------
    list
        拆分后的list
    """
    for i in range(0, len(old_list), n_len):
        yield old_list[i : i + n_len]


def make_dirs(path: str):
    """
    判断目录是否存在,不存在则创建

    Parameters
    ----------
    path : str
        目录地址
    """
    # 判断是否存在文件夹如果不存在则创建为文件夹
    if not os.path.exists(path):
        # makedirs 创建文件时如果路径不存在会创建这个路径
        try:
            os.makedirs(path)
        except Exception as e:
            print(e)


def cut_series_to_group(
    series: pd.Series, q: int = 10, ascending: bool = False
) -> pd.Series:
    """
    将pd.Series分成q组,如果数据长度小于q则全部为1

    Parameters
    ----------
    series : pd.Series
        需要分组的series
    q : int, optional
        需要分成的组数, by default 10
    ascending : bool, optional
        是否升序排序, by default False

    Returns
    -------
    pd.Series
        分组结果的series
    """
    labels = range(0, q + 1, 1) if ascending else range(q, 0, -1)
    if series.shape[0] < q:
        group_series = pd.Series(
            [1] * len(series), index=series.index, name=series.name
        )

    else:
        group_series = pd.Series(
            pd.qcut(series.rank(method="first"), q=q, labels=labels)
        )
    group_series = group_series.astype(int)
    return group_series


def cal_series_rank(
    series: pd.Series, ascending: bool = False, if_pct: bool = False
) -> pd.Series:
    """
    计算pd.Series的排名格式为n/m

    Parameters
    ----------
    series : pd.Series
        需要分组的series
    ascending : bool, optional
        是否升序排序, by default False

    Returns
    -------
    pd.Series
        排名结果的series
    """

    if if_pct:
        return series.rank(ascending=ascending, method="first", pct=if_pct) * 100
    else:
        series_rank = series.rank(ascending=ascending, method="first")
        series_len = series_rank.shape[0]
        return series_rank.apply(lambda x: f"{int(x)}/{series_len}")


def change_1min_to_mins(df_1min: pd.DataFrame, N: str = "30min") -> pd.DataFrame:
    """
    将1分钟线变化为其他级别分钟线,例如["5min", "15min", "30min", "60min"]

    Parameters
    ----------
    df_1min : pd.DataFrame
        1分钟的DataFrame
    N : str, optional
        其他级别的分钟属性, by default '30min'

    Returns
    -------
    pd.DataFrame
        index为date_time,
        columns=["open", "high", "low", "close", "volume", "amount"]
    """
    if N not in ["5min", "15min", "30min", "60min"]:
        raise Exception("目标周期不合法")

    if N == "60min":
        df = (
            df_1min.resample("30min", closed="right", label="right")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "turnover": "sum",
                    "amount": "sum",
                }
            )
            .dropna()
        )
        df = df.reset_index()
        idx = list(range(0, len(df) // 2)) * 2
        idx.sort()
        df["idx"] = idx
        df = df.groupby(by="idx").agg(
            {
                "trade_time": "last",
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "turnover": "sum",
                "amount": "sum",
            }
        )
        df.set_index("trade_time", inplace=True)
    else:
        df = (
            df_1min.resample(N, closed="right", label="right")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "turnover": "sum",
                    "amount": "sum",
                }
            )
            .dropna()
        )
    return df


def change_1min_to_day(df_1min: pd.DataFrame):
    return (
        df_1min.resample("D")
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "turnover": "sum",
                "amount": "sum",
            }
        )
        .dropna()
    )


def change_1min_to_other(df_1min: pd.DataFrame, N: str = "30min"):

    if (df_1min.shape[0] % 240) != 0:
        print(f"{df_1min.code[0]}分钟数据不满足240条记录")
        return None

    if N not in ["5min", "15min", "30min", "60min", "day"]:
        raise Exception("目标周期不合法")
    try:
        if N == "day":
            return (
                df_1min.resample("D")
                .agg(
                    {
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "turnover": "sum",
                        "amount": "sum",
                    }
                )
                .dropna()
            )

        if N == "60min":
            df = (
                df_1min.resample("30min", closed="right", label="right")
                .agg(
                    {
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "turnover": "sum",
                        "amount": "sum",
                    }
                )
                .dropna()
            )
            df = df.reset_index()
            idx = list(range(0, len(df) // 2)) * 2
            idx.sort()
            df["idx"] = idx
            df = df.groupby(by="idx").agg(
                {
                    "trade_time": "last",
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "turnover": "sum",
                    "amount": "sum",
                }
            )
            df.set_index("trade_time", inplace=True)
        else:
            df = (
                df_1min.resample(N, closed="right", label="right")
                .agg(
                    {
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "turnover": "sum",
                        "amount": "sum",
                    }
                )
                .dropna()
            )
        return df
    except Exception as e:
        raise Exception(f"{df_1min.code.values[0]}分钟数据时间戳不正确")
