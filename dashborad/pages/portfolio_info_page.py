import dash
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pandas as pd
from dash import dcc, html
from src.constant import CONTENT_STYLE

import quant_utils.data_moudle as dm
from dashborad.src.ids import ids

dash.register_page(
    __name__,
    path="/portfolio_info",
    title="Portfolio Info",
    name="组合信息",
    order=1,
)

ID = ids["portfolio_info_page"]


def layout(**kwargs) -> html.Div:
    return html.Div(
        [

            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H1(
                                "组合信息",
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
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dcc.Loading(
                                id="PORTFOLIO_INFO_LOADING",
                                type="circle",
                                fullscreen=True,
                                children=[
                                    html.Div(
                                        id=ID["PORTFOLIO_INFO_TABLE"],
                                        children=get_portfolio_info_table(),
                                    ),
                                ],
                            )
                        ]
                    )
                ]
            ),
        ],
        style=CONTENT_STYLE,
    )


def get_portfolio_info_table() -> dag.AgGrid:
    df = dm.get_portfolio_daily_limit()
    if df.empty:
        return html.Div("没有数据")
    # 定义列的配置
    columnDefs = [
        {"headerName": col, "field": col, "filter": True, "sortable": True}
        for col in df.columns
    ]
    return dag.AgGrid(
        columnDefs=columnDefs,
        rowData=df.to_dict("records"),
        defaultColDef={
            "resizable": True,
            "sortable": True,
            "filter": True,
            "editable": False,
            "wrapText": True,
            "autoHeight": True,
            "autoSize": True,
            "cellStyle": {"text-align": "center"},
            "headerCellRendererParams": {"style": {"text-align": "center"}},
        },
        style={"height": "600px", "width": "100%"},
        className="ag-theme-alpine",
    )
