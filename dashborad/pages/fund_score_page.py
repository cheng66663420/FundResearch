import base64
from io import BytesIO

import dash
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, State, callback, dcc, html
from src import constant

import quant_utils.data_moudle as dm
from dashborad.src.ids import ids
from quant_utils.constant_varialbles import LAST_TRADE_DT

dash.register_page(
    __name__,
    path="/fund_score",
    title="Fund Score",
    name="基金评分",
    order=2,
)

ID = ids["fund_score_page"]


def layout(**kwargs) -> html.Div:
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H1(
                                "基金评分信息",
                                className="text-center mb-1 p-1",
                                style=constant.HEADER_STYLE,
                            )
                        ]
                    ),
                ],
                justify="center",
            ),
            html.Br(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Button(
                                children="点击查询基金评分",
                                id=ID["FUND_SCORE_QUERY_BUTTON"],
                                n_clicks=0,
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Button(
                                children="点击下载当前数据",
                                id=ID["FUND_SCORE_DOWNLOAD_BUTTON"],
                                n_clicks=0,
                            ),
                            dcc.Download(
                                id=ID["FUND_SCORE_DOWNLOAD"],
                            ),
                        ],
                        width=3,
                    ),
                ]
            ),
            html.Br(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dcc.Loading(
                                id="fund_score_loading",
                                children=[
                                    html.Div(id=ID["FUND_SCORE_TABLE"], children=[])
                                ],
                            ),
                        ]
                    )
                ]
            ),
        ],
        style=constant.CONTENT_STYLE,
    )


@callback(
    Output(ID["FUND_SCORE_TABLE"], "children"),
    Input(ID["FUND_SCORE_QUERY_BUTTON"], "n_clicks"),
)
def create_fund_score_table(n_clicks) -> html.Div:
    if n_clicks == 0:
        return html.Div("请点击查询按钮")
    fund_score = dm.get_fund_score(LAST_TRADE_DT)

    if fund_score.empty:
        return html.Div("没有数据")
    # 定义列的配置
    columnDefs = [
        {"headerName": col, "field": col, "filter": True, "sortable": True}
        for col in fund_score.columns
    ]
    return html.Div(
        children=[
            dag.AgGrid(
                columnDefs=columnDefs,
                rowData=fund_score.to_dict("records"),
                defaultColDef={
                    "resizable": True,
                    "sortable": True,
                    "filter": True,
                    "editable": False,
                    "wrapText": True,
                    "autoHeight": True,
                    "width": 145,
                    "cellStyle": {"text-align": "center"},
                },
                style={"height": "800px", "width": "100%"},
                className="ag-theme-alpine",
            )
        ]
    )


@callback(
    Output(ID["FUND_SCORE_DOWNLOAD"], "data"),
    [
        Input(ID["FUND_SCORE_DOWNLOAD_BUTTON"], "n_clicks"),
    ],
    [
        State(ID["FUND_SCORE_TABLE"], "children"),
    ],
    prevent_initial_call=True,
)
def download_fund_score_table(_, table: dict):
    buffer = BytesIO()
    try:
        df = pd.DataFrame(table["props"]["children"][0]["props"]["rowData"])
    except TypeError:
        print(TypeError)
        df = pd.DataFrame()

    # 使用 Pandas 的 ExcelWriter 将多个 DataFrame 写入同一个 Excel 文件的不同工作表
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="基金打分", index=False)
    # 获取二进制数据并编码为 base64
    excel_data = buffer.getvalue()
    b64 = base64.b64encode(excel_data).decode()

    # 返回带有 base64 编码的 Excel 数据的下载链接
    return dcc.send_bytes(excel_data, "基金打分.xlsx")
