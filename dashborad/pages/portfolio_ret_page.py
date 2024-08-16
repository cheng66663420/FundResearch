import base64
import datetime
from io import BytesIO

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, callback, dash_table, dcc, html
from dash.dash_table import FormatTemplate
from dash.dash_table.Format import Format
from src.constant import CONTENT_STYLE
from src.data.datasource import PortRetDataSource

import quant_utils.data_moudle as dm
from dashborad.src.ids import ids
from portfolio.portfolio_product_contribution import (
    get_portfolios_products_contribution,
)
from quant_utils.constant_varialbles import LAST_TRADE_DT

dash.register_page(__name__, order=0, name="组合概览", path="/")

# PORTFOLIO_INFO = dm.get_portfolio_info()
# PORTFOLIO_INFO = PORTFOLIO_INFO.query(
#     "IF_LISTED == 1 and LISTED_DATE <= @LAST_TRADE_DT and PORTFOLIO_TYPE != '目标盈'"
# )
# PORTFOLIO_TYPE_LIST = ["全部", "短期周转", "稳健理财", "财富增长"]  # , "目标盈"]
# PORTFOLIO_NAME_LIST = PORTFOLIO_INFO["PORTFOLIO_NAME"].unique().tolist()
ID = ids["portfolio_ret_page"]
STYLE = {
    # "width": "100%",
    # "height": "40px",
    # "text-align": "center",
    "font-size": "16px",
    "font-weight": "bold",
    "color": "#333",
    "background-color": "#fff",
    # "border": "1px solid #ccc",
}


def layout(**kargs) -> html.Div:
    return html.Div(
        [
            dcc.Location(id=ID["PORTFOLIO_RET_PAGE_URL"], refresh=False),
            dcc.Store(id=ID["PORTFOLIO_INFO_STORE"], storage_type="memory"),
            dcc.Store(id=ID["PORTFOLIO_RET_STORE"], storage_type="memory"),
            dcc.Store(id=ID["PORTFILIO_DATES_STORE"], storage_type="memory"),
            dcc.Store(id=ID["PORTFOLIO_RET_PERFORMANCE_STORE"], storage_type="memory"),
            # 页面标题
    
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H1(
                                "组合收益概览",
                                className="text-center mb-1 p-1",
                                style={
                                    "color": "#921235",
                                    "font-size": "30px",
                                    "font-weight": "bold",
                                },
                            )
                        ]
                    ),
                ],
                justify="center",
            ),
            html.Br(),
            # 组合类型、名称、时间筛选
            dbc.Row(
                [
                    dbc.Col(
                        [
                            port_ret_dropdowns(),
                        ]
                    )
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Button(
                                children="点击下载当前数据",
                                id=ID["PORTFOLIO_RET_DOWNLOAD_BUTTON"],
                                n_clicks=0,
                            ),
                            dcc.Download(
                                id=ID["PORTFOLIO_RET_DOWNLOAD"],
                            ),
                        ]
                    ),
                ]
            ),
            html.Br(),
            # 组合收益数据表格
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(id=ID["PORTFOLIO_PERFORMANCE_TABLE"], children=[]),
                        ],
                    ),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            # 组合收益图表
                            dcc.Graph(
                                id=ID["PORTFOLIO_RETURN_GRAGH"],
                                style={"height": 450},
                                className="shadow-lg",
                            ),
                        ],
                        width=6,
                    ),
                    dbc.Col(
                        [
                            # 最大回撤图
                            dcc.Graph(
                                id=ID["PORTFOLIO_DRAWDOWN"],
                                style={"height": 450},
                                className="shadow-lg",
                            ),
                        ],
                        width=6,
                    ),
                ],
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dcc.RadioItems(
                                id=ID["PORTFOLIO_RET_RADIO_TYPE_ITEM"],
                                options=["全部", "指标", "时间区间"],
                                value="指标",
                                labelStyle={
                                    "display": "inline-block",
                                    "margin-right": "10px",
                                },
                            ),
                            html.Br(),
                            dcc.RadioItems(
                                id=ID["PORTFOLIO_RET_RADIO_ITEM"],
                                labelStyle={
                                    "display": "inline-block",
                                    "margin-right": "10px",
                                },
                            ),
                            html.Br(),
                            dcc.Loading(
                                id="loading-1",
                                children=html.Div(
                                    id=ID["PORTFOLIO_RET_PERFORMANCE_TABLE_2"]
                                ),
                                type="circle",
                            ),
                        ],
                    ),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Button(
                                children="点击查询绩效分析",
                                id=ID["PORTFOLIO_ATTRIBUTE_BUTTON"],
                                n_clicks=0,
                                style={
                                    # "color": "#921235",
                                    "font-size": "20px",
                                    "font-weight": "bold",
                                },
                            ),
                        ]
                    )
                ]
            ),
            dbc.Row(
                [
                    html.H6(
                        "总收益",
                        className="p-3",
                        style={
                            "color": "#921235",
                            "font-size": "20px",
                            "font-weight": "bold",
                        },
                    ),
                    dbc.Col(
                        [
                            dcc.Loading(
                                id="portflo_ret_loading-2",
                                children=[
                                    html.Div(id=ID["PORTFOLIO_ATTRIBUTE_TOTAL_RET"])
                                ],
                            )
                        ]
                    ),
                    html.Br(),
                    html.H6(
                        "大类贡献",
                        className="p-3",
                        style={
                            "color": "#921235",
                            "font-size": "20px",
                            "font-weight": "bold",
                        },
                    ),
                    dbc.Col(
                        [
                            dcc.Loading(
                                id="portflo_ret_loading-3",
                                children=[html.Div(id=ID["PORTFOLIO_ATTRIBUTE_ALPHA"])],
                            )
                        ]
                    ),
                    html.Br(),
                    html.H6(
                        "基金贡献",
                        className="p-3",
                        style={
                            "color": "#921235",
                            "font-size": "20px",
                            "font-weight": "bold",
                        },
                    ),
                    dbc.Col(
                        [
                            dcc.Loading(
                                id="portflo_ret_loading-4",
                                children=[html.Div(id=ID["PORTFOLIO_ATTRIBUTE_FUND"])],
                            )
                        ]
                    ),
                ]
            ),
        ],
        style=CONTENT_STYLE,
    )


@callback(
    Output(ID["PORTFOLIO_INFO_STORE"], "data"),
    [
        Input(ID["PORTFOLIO_RET_PAGE_URL"], "pathname"),
    ],
)
def update_portfolio_info_store(_: str) -> dict:
    """
    更新组合信息数据

    Parameters
    ----------
    _ : str
        URL路径

    Returns
    -------
    dict
        组合信息数据
    """
    portfolio_info = dm.get_portfolio_info()
    portfolio_info = portfolio_info.query(
        "IF_LISTED == 1 and LISTED_DATE <= @LAST_TRADE_DT"
    )
    return portfolio_info.to_dict("records")


@callback(
    [
        Output(ID["PORTFILIO_DATES_STORE"], "data"),
        Output(ID["PORTFOLIO_DATES_NAME"], "options"),
    ],
    [
        Input(ID["PORTFOLIO_NAME_DROPDOWN"], "value"),
        Input(ID["PORTFOLIO_END_PICKER"], "value"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def update_portfolio_dates_store(portfolio_name: str, end_date: str, _: dict) -> tuple:
    """
    更新组合时间区间数据

    Parameters
    ----------
    portfolio_name : str
        组合名称
    end_date : str
        结束日期

    Returns
    -------
    tuple
        (组合收益数据, 组合日期数据)
    """
    portfolio_dates_df = dm.get_portfolio_dates_name(
        portfolio_name=portfolio_name, end_date=end_date
    )
    return (
        portfolio_dates_df.to_dict("records"),
        portfolio_dates_df["DATE_NAME"].tolist(),
    )


@callback(
    Output(ID["PORTFOLIO_START_PICKER"], "value"),
    [
        Input(ID["PORTFOLIO_DATES_NAME"], "value"),
        Input(ID["PORTFILIO_DATES_STORE"], "data"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def update_portfolio_start_picker(date_name: str, date_name_dict: dict, _: dict) -> str:
    """
    更新组合开始日期选择器

    Parameters
    ----------
    date_name : str
        日期名称

    Returns
    -------
    str
        开始日期
    """

    df = pd.DataFrame(date_name_dict)
    try:
        return df[df["DATE_NAME"] == date_name]["START_DATE"].values[0]
    except:
        raise dash.exceptions.PreventUpdate


@callback(
    [
        Output(ID["PORTFOLIO_RET_STORE"], "data"),
        Output(ID["PORTFOLIO_START_PICKER"], "options"),
        Output(ID["PORTFOLIO_END_PICKER"], "options"),
        Output(ID["PORTFOLIO_END_PICKER"], "value"),
        Output(ID["PORTFOLIO_DATES_NAME"], "value"),
    ],
    [
        Input(ID["PORTFOLIO_NAME_DROPDOWN"], "value"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def update_portfolio_ret_store(portfolio_name: str, _: dict) -> tuple:
    """
    更新组合收益数据

    Parameters
    ----------
    portfolio_name : str
        组合名称

    Returns
    -------
    """
    portfolio_ret = dm.get_listed_portfolio_derivatives_ret(
        portfolio_name=portfolio_name
    ).reset_index()
    portfolio_ret = PortRetDataSource(portfolio_ret)
    trade_date_list = portfolio_ret.trade_dates

    return (
        portfolio_ret.to_dict("records"),
        trade_date_list,
        trade_date_list,
        trade_date_list[-1],
        "成立日",
    )


@callback(
    [
        Output(ID["PORTFOLIO_RETURN_GRAGH"], "figure"),
        Output(ID["PORTFOLIO_DRAWDOWN"], "figure"),
    ],
    [
        Input(ID["PORTFOLIO_RET_STORE"], "data"),
        Input(ID["PORTFOLIO_START_PICKER"], "value"),
        Input(ID["PORTFOLIO_END_PICKER"], "value"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def update_graph(
    portfolio_ret: dict, start_date: str, end_date: str, _: dict
) -> go.Figure:
    """
    根据组合名称更新收益图

    Parameters
    ----------
    portfolio_name : _type_
        组合名称

    Returns
    -------
    go.Figure
        收益图
    """
    portfolio_ret = pd.DataFrame(portfolio_ret)
    if portfolio_ret.empty:
        return go.Figure().update_layout(
            paper_bgcolor="#ffffff ",
            plot_bgcolor="#ffffff ",
        ), go.Figure().update_layout(
            paper_bgcolor="#ffffff ",
            plot_bgcolor="#ffffff ",
        )

    portfolio_ret = PortRetDataSource(portfolio_ret)
    portfolio_ret = portfolio_ret.filter_by_date(start_date, end_date)
    portfolio_ret = portfolio_ret.normlized()
    return create_portfolio_ret_chart(portfolio_ret.data), create_drawdown_chart(
        portfolio_ret.data
    )


@callback(
    [
        Output(ID["PORTFOLIO_TYPE_DROPDOWN"], "options"),
        Output(ID["PORTFOLIO_TYPE_DROPDOWN"], "value"),
    ],
    [
        Input(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def update_portfolio_type_options(portfolio_info: dict) -> tuple:
    df = pd.DataFrame(portfolio_info)
    return ["全部"] + df["PORTFOLIO_TYPE"].unique().tolist(), "全部"


@callback(
    [
        Output(ID["PORTFOLIO_NAME_DROPDOWN"], "options"),
        Output(ID["PORTFOLIO_NAME_DROPDOWN"], "value"),
    ],
    [
        Input(ID["PORTFOLIO_TYPE_DROPDOWN"], "value"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def select_portfolio_type(
    portfolio_type: str, portfolio_info: dict
) -> tuple[list, str]:
    """
    根据选择的组合类型更新组合名称

    Parameters
    ----------
    value : str
        _description_

    Returns
    -------
    tuple[list, str]
        (组合名称列表, 列表中第一个组合名称)
    """
    df = pd.DataFrame(portfolio_info)

    if portfolio_type == "全部":
        name_list = df["PORTFOLIO_NAME"].tolist()

        return name_list, name_list[0]
    else:

        name_list2 = df.query("PORTFOLIO_TYPE == @portfolio_type")[
            "PORTFOLIO_NAME"
        ].tolist()

        return name_list2, name_list2[0]


@callback(
    Output(ID["PORTFOLIO_PERFORMANCE_TABLE"], "children"),
    [
        Input(ID["PORTFOLIO_RET_STORE"], "data"),
        Input(ID["PORTFOLIO_START_PICKER"], "value"),
        Input(ID["PORTFOLIO_END_PICKER"], "value"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def update_portfolio_performance_table(
    portfolio_ret: dict, start_date: str, end_date: str, _: dict
) -> dash_table.DataTable:
    dff = PortRetDataSource(pd.DataFrame(portfolio_ret))
    dff = dff.filter_by_date(start_date, end_date).get_performance()
    columns = []
    for col in dff.columns:
        if col in ["累计收益率", "年化收益率", "年化波动率", "最大回撤"]:
            columns.append(
                {
                    "name": col,
                    "id": col,
                    "type": "numeric",
                    "format": FormatTemplate.percentage(2),
                }
            )
        elif col in ["收益波动比", "年化收益回撤比"]:
            columns.append(
                {
                    "name": col,
                    "id": col,
                    "type": "numeric",
                    "format": Format(decimal_delimiter=".").scheme("f").precision(2),
                }
            )
        else:
            columns.append({"name": col, "id": col})

    portfolio_performance_table = dash_table.DataTable(
        columns=columns,
        data=dff.to_dict("records"),
        style_header={
            "font-weight": "bold",
            "textAlign": "center",
            "font-size": "16px",
            "background-color": "rgb(242,242,242)",
        },
        style_data={"textAlign": "center"},
    )
    return portfolio_performance_table


@callback(
    Output(ID["PORTFOLIO_RET_DOWNLOAD"], "data"),
    [
        Input(ID["PORTFOLIO_RET_DOWNLOAD_BUTTON"], "n_clicks"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
        State(ID["PORTFOLIO_RET_STORE"], "data"),
        State(ID["PORTFOLIO_START_PICKER"], "value"),
        State(ID["PORTFOLIO_END_PICKER"], "value"),
        State(ID["PORTFOLIO_NAME_DROPDOWN"], "value"),
        State(ID["PORTFOLIO_PERFORMANCE_TABLE"], "children"),
        State(ID["PORTFOLIO_RET_PERFORMANCE_STORE"], "data"),
        State(ID["PORTFOLIO_ATTRIBUTE_TOTAL_RET"], "children"),
        State(ID["PORTFOLIO_ATTRIBUTE_ALPHA"], "children"),
        State(ID["PORTFOLIO_ATTRIBUTE_FUND"], "children"),
    ],
    prevent_initial_call=True,
)
def download_portfolio_ret(
    n_clicks: int,
    portfolio_info: dict,
    portfolio_ret: dict,
    start_date: str,
    end_date: str,
    portfolio_name: str,
    perf_table: dict,
    perf_indicators: dict,
    attribute_ret: dict,
    attribute_alpha: dict,
    attribute_fund: dict,
):
    buffer = BytesIO()

    portfolio_ret = PortRetDataSource(pd.DataFrame(portfolio_ret))
    portfolio_ret = portfolio_ret.filter_by_date(start_date, end_date).normlized()
    # print(perf_table)
    try:
        perf = pd.DataFrame(perf_table["props"]["data"])
    except TypeError:
        print(TypeError)
        perf = pd.DataFrame()
    try:
        attribute_ret_df = pd.DataFrame(attribute_ret["props"]["data"])
        attribute_alpha_df = pd.DataFrame(attribute_alpha["props"]["data"])
        attribute_fund_df = pd.DataFrame(attribute_fund["props"]["data"])
    except TypeError:
        attribute_ret_df = pd.DataFrame()
        attribute_alpha_df = pd.DataFrame()
        attribute_fund_df = pd.DataFrame()

    try:
        perf_indicators = pd.DataFrame(perf_indicators)
    except TypeError:
        perf_indicators = pd.DataFrame()

    # 使用 Pandas 的 ExcelWriter 将多个 DataFrame 写入同一个 Excel 文件的不同工作表
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        portfolio_ret.data.to_excel(writer, sheet_name="组合收益序列", index=False)
        perf.to_excel(writer, sheet_name="总区间绩效指标", index=False)
        perf_indicators.to_excel(writer, sheet_name="多重指标", index=False)
        attribute_ret_df.to_excel(writer, sheet_name="归因_总收益", index=False)
        attribute_alpha_df.to_excel(writer, sheet_name="归因_大类资产", index=False)
        attribute_fund_df.to_excel(writer, sheet_name="归因_基金", index=False)
    # 获取二进制数据并编码为 base64
    excel_data = buffer.getvalue()
    b64 = base64.b64encode(excel_data).decode()

    # 返回带有 base64 编码的 Excel 数据的下载链接
    return dcc.send_bytes(excel_data, f"{portfolio_name}.xlsx")


@callback(
    Output(ID["PORTFOLIO_RET_PERFORMANCE_STORE"], "data"),
    [
        Input(ID["PORTFOLIO_END_PICKER"], "value"),
        Input(ID["PORTFOLIO_NAME_DROPDOWN"], "value"),
        Input(ID["PORTFOLIO_INFO_STORE"], "data"),
    ],
)
def update_portfolio_ret_performance(
    end_date: str,
    portfolio_name: str,
    portfolio_info: dict,
):
    performance_df = dm.get_portfolio_derivatives_perfomance(
        portfolio_name=portfolio_name, end_date=end_date
    )
    return performance_df.to_dict("records")


@callback(
    [
        Output(ID["PORTFOLIO_RET_RADIO_ITEM"], "options"),
        Output(ID["PORTFOLIO_RET_RADIO_ITEM"], "value"),
    ],
    [
        Input(ID["PORTFOLIO_INFO_STORE"], "data"),
        Input(ID["PORTFOLIO_RET_RADIO_TYPE_ITEM"], "value"),
        Input(ID["PORTFOLIO_RET_PERFORMANCE_STORE"], "data"),
    ],
)
def update_portfolio_ret_performance_radio_items(
    portfolio_info: dict,
    radio_type: str,
    performance_data: dict,
) -> tuple:
    if radio_type == "全部":
        return [], ""

    performance_df = pd.DataFrame(performance_data)
    if performance_df.empty:
        return [], ""
    radio_items_options = performance_df[radio_type].unique().tolist()
    return (
        radio_items_options,
        radio_items_options[0],
    )


@callback(
    Output(ID["PORTFOLIO_RET_PERFORMANCE_TABLE_2"], "children"),
    [
        Input(ID["PORTFOLIO_RET_RADIO_ITEM"], "value"),
        Input(ID["PORTFOLIO_RET_PERFORMANCE_STORE"], "data"),
        Input(ID["PORTFOLIO_INFO_STORE"], "data"),
        Input(ID["PORTFOLIO_RET_RADIO_TYPE_ITEM"], "value"),
    ],
)
def update_portfolio_ret_performance_table_2(
    indicator_name: str,
    performance_data: dict,
    portfolio_info: dict,
    radio_type: str,
) -> dash.dash_table.DataTable:
    performance_df = pd.DataFrame(performance_data)
    if performance_df.empty:
        return html.Div("结束日期暂无数据", style={"font-weiget": "bold"})

    if radio_type != "全部":
        performance_df = performance_df[performance_df[radio_type] == indicator_name]

    columns = [{"name": i, "id": i} for i in performance_df.columns]
    performance_table = dash_table.DataTable(
        columns=columns,
        data=performance_df.to_dict("records"),
        page_size=15,
        filter_action="native",
        sort_action="native",  # 使用 Dash 内置的排序功能
        sort_mode="multi",  # 允许对多列进行排序
        style_header={
            "font-weight": "bold",
            "textAlign": "center",
            "font-size": "16px",
            "background-color": "rgb(242,242,242)",
        },
        style_data={"textAlign": "center"},
    )
    return performance_table


@callback(
    [
        Output(ID["PORTFOLIO_ATTRIBUTE_TOTAL_RET"], "children"),
        Output(ID["PORTFOLIO_ATTRIBUTE_ALPHA"], "children"),
        Output(ID["PORTFOLIO_ATTRIBUTE_FUND"], "children"),
    ],
    [
        Input(ID["PORTFOLIO_ATTRIBUTE_BUTTON"], "n_clicks"),
    ],
    [
        State(ID["PORTFOLIO_INFO_STORE"], "data"),
        State(ID["PORTFOLIO_NAME_DROPDOWN"], "value"),
        State(ID["PORTFOLIO_START_PICKER"], "value"),
        State(ID["PORTFOLIO_END_PICKER"], "value"),
    ],
    prevent_initial_call=True,
)
def update_portfolio_attribute(
    n_clicks: int,
    portfolio_info: dict,
    portfolio_name: str,
    start_date: str,
    end_date: str,
):
    fund_ret, alpha_ret, sum_ret = get_portfolios_products_contribution(
        start_date=start_date,
        end_date=end_date,
        portfolio_list=[portfolio_name],
        level_num=1,
    )
    fund_ret_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in fund_ret.columns],
        data=fund_ret.to_dict("records"),
        # page_size=15,
        # filter_action="native",
        # sort_action="native",  # 使用 Dash 内置的排序功能
        # sort_mode="multi",  # 允许对多列进行排序
        style_header={
            "font-weight": "bold",
            "textAlign": "center",
            "font-size": "16px",
            "background-color": "rgb(242,242,242)",
        },
        style_data={"textAlign": "center"},
    )

    alpha_ret_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in alpha_ret.columns],
        data=alpha_ret.to_dict("records"),
        # page_size=15,
        # filter_action="native",
        # sort_action="native",  # 使用 Dash 内置的排序功能
        # sort_mode="multi",  # 允许对多列进行排序
        style_header={
            "font-weight": "bold",
            "textAlign": "center",
            "font-size": "16px",
            "background-color": "rgb(242,242,242)",
        },
        style_data={"textAlign": "center"},
    )
    sum_ret_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in sum_ret.columns],
        data=sum_ret.to_dict("records"),
        # page_size=15,
        # filter_action="native",
        # sort_action="native",  # 使用 Dash 内置的排序功能
        # sort_mode="multi",  # 允许对多列进行排序
        style_header={
            "font-weight": "bold",
            "textAlign": "center",
            "font-size": "16px",
            "background-color": "rgb(242,242,242)",
        },
        style_data={"textAlign": "center"},
    )

    return sum_ret_table, alpha_ret_table, fund_ret_table


def port_ret_dropdowns() -> html.Div:
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.P("组合类型", className="text-center"),
                            dcc.Dropdown(
                                id=ID["PORTFOLIO_TYPE_DROPDOWN"],
                                clearable=False,
                                style={"text-align": "center"},
                            ),
                        ],
                        width={"size": 3, "offset": 0, "order": 0},
                        style=STYLE,
                    ),
                    dbc.Col(
                        [
                            html.P("组合名称", className="text-center"),
                            dcc.Dropdown(
                                id=ID["PORTFOLIO_NAME_DROPDOWN"],
                                # options=PORTFOLIO_NAME_LIST,
                                # value=PORTFOLIO_NAME_LIST[0],
                                clearable=False,
                                style={"text-align": "center"},
                            ),
                        ],
                        width={"size": 3, "offset": 0, "order": 0},
                        style=STYLE,
                    ),
                    dbc.Col(
                        [
                            html.P("时间区间", className="text-center"),
                            dcc.Dropdown(
                                id=ID["PORTFOLIO_DATES_NAME"],
                                value="成立日",
                                clearable=False,
                                style={"text-align": "center"},
                            ),
                        ],
                        style=STYLE,
                    ),
                    dbc.Col(
                        [
                            html.P("开始时间", className="text-center"),
                            dcc.Dropdown(
                                id=ID["PORTFOLIO_START_PICKER"],
                                # className="dropdown-centered",
                                clearable=False,
                                style={"text-align": "center"},
                            ),
                        ],
                        style=STYLE,
                    ),
                    dbc.Col(
                        [
                            html.P("结束时间", className="text-center"),
                            dcc.Dropdown(
                                id=ID["PORTFOLIO_END_PICKER"],
                                clearable=False,
                                style={"text-align": "center"},
                            ),
                        ],
                        style=STYLE,
                    ),
                ],
            ),
        ]
    )


def create_portfolio_ret_chart(portfolio_ret: pd.DataFrame) -> go.Figure:
    """
    创建组合收益率图表

    Parameters
    ----------
    portfolio_ret : pd.DataFrame
        组合收益的数据

    Returns
    -------
    go.Figure
        组合收益图包含累计收益率和基准累计收益率和超额收益
    """
    portfolio_ret["TRADE_DT"] = pd.to_datetime(portfolio_ret["TRADE_DT"])
    chart_ptfvalue = go.Figure()
    chart_ptfvalue.add_trace(
        go.Scatter(
            x=portfolio_ret["TRADE_DT"],
            y=portfolio_ret["PORTFOLIO_RET_ACCUMULATED"],
            mode="lines",  # you can also use "lines+markers", or just "markers"
            name="组合累计收益率",
            hovertemplate="%{y:.2f}%",
            line=dict(color="rgba(163, 25, 57, 1)", width=2),
        )
    )

    chart_ptfvalue.add_trace(
        go.Scatter(
            x=portfolio_ret["TRADE_DT"],
            y=portfolio_ret["BENCHMARK_RET_ACCUMULATED_INNER"],
            # fill="tozeroy",
            # fillcolor="rgba(64, 129, 193, 1)",  # https://www.w3schools.com/css/css_colors_rgb.asp
            line=dict(color="rgba(64, 129, 193, 1)", width=2),
            mode="lines",  # you can also use "lines+markers", or just "markers"
            name="业绩比较基准",
            hovertemplate="%{y:.2f}%",
        )
    )

    chart_ptfvalue.add_trace(
        go.Scatter(
            x=portfolio_ret["TRADE_DT"],
            y=portfolio_ret["ALPHA"],
            fill="tozeroy",
            fillcolor="rgba(225,208,225, 0.5)",  # https://www.w3schools.com/css/css_colors_rgb.asp
            line=dict(color="rgba(225,208,225, 0.5)", width=2),
            mode="lines",  # you can also use "lines+markers", or just "markers"
            name="超额收益率",
            hovertemplate="%{y:.2f}%",
        )
    )

    chart_ptfvalue.update_layout(
        title="累计收益率%",
        title_font=dict(
            size=20,
            family="Arial Black",
        ),
        margin=dict(
            t=50, b=50, l=50, r=50
        ),  # this will help you optimize the chart space
        xaxis_tickfont_size=12,
        yaxis=dict(
            title="累计收益率: %",
            titlefont_size=14,
            tickfont_size=12,
        ),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        showlegend=True,
        #     title='Global Portfolio Value (USD $)',
        title_x=0.5,  # title centered
        paper_bgcolor="#ffffff ",
        plot_bgcolor="#ffffff ",
    )

    # Time Series with Range Selector Buttons - https://plotly.com/python/time-series/
    chart_ptfvalue.update_xaxes(
        tickformat="%Y-%m-%d",
        # dtick="M3",
        rangeslider_visible=False,
        # rangeselector=dict(
        #     buttons=list(
        #         [
        #             dict(count=7, label="1w", step="day", stepmode="backward"),
        #             dict(count=14, label="2w", step="day", stepmode="backward"),
        #             dict(count=1, label="1m", step="month", stepmode="backward"),
        #             dict(count=6, label="6m", step="month", stepmode="backward"),
        #             dict(count=12, label="12m", step="month", stepmode="backward"),
        #             dict(count=1, label="YTD", step="year", stepmode="todate"),
        #             dict(label="All", step="all"),
        #         ]
        #     ),
        #     bgcolor="#eaeaea",
        #     activecolor="tomato",
        #     # y=1.22,
        #     # x=0.25,
        # ),
    )
    chart_ptfvalue.update_yaxes(
        tickformat=".2f",
        ticksuffix="%",
    )
    chart_ptfvalue.update_layout(hovermode="x unified")
    # chart_ptfvalue.layout.template = CHART_THEME
    return chart_ptfvalue


def create_drawdown_chart(portfolio_ret: pd.DataFrame) -> go.Figure:
    """
    创建组合最大回撤图

    Parameters
    ----------
    portfolio_ret : pd.DataFrame
        组合收益数据

    Returns
    -------
    go.Figure
        最大回撤图
    """
    portfolio_ret["TRADE_DT"] = pd.to_datetime(portfolio_ret["TRADE_DT"])
    chart_ptfvalue = go.Figure()
    chart_ptfvalue.add_trace(
        go.Scatter(
            x=portfolio_ret["TRADE_DT"],
            y=portfolio_ret["MAX_DRAWDOWN"],
            fill="tozeroy",
            fillcolor="rgba(163, 25, 57, 0.8)",
            mode="lines",  # you can also use "lines+markers", or just "markers"
            name="最大回撤",
            hovertemplate="%{y:.2f}%",
            line=dict(color="rgba(163, 25, 57, 0.8)", width=2),
        )
    )
    chart_ptfvalue.update_layout(
        title="最大回撤%",
        title_font=dict(
            size=20,
            family="Arial Black",
        ),
        margin=dict(
            t=50, b=50, l=50, r=50
        ),  # this will help you optimize the chart space
        xaxis_tickfont_size=12,
        yaxis=dict(
            title="最大回撤: %",
            titlefont_size=14,
            tickfont_size=12,
        ),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        showlegend=False,
        title_x=0.5,  # title centered
        paper_bgcolor="#ffffff ",
        plot_bgcolor="#ffffff ",
    )

    # Time Series with Range Selector Buttons - https://plotly.com/python/time-series/
    chart_ptfvalue.update_xaxes(
        tickformat="%Y-%m-%d",
        rangeslider_visible=False,
    )
    chart_ptfvalue.update_yaxes(
        tickformat=".2f",
        ticksuffix="%",
    )
    chart_ptfvalue.update_layout(hovermode="x unified")
    # chart_ptfvalue.layout.template = CHART_THEME
    return chart_ptfvalue
