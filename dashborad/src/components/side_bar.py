import dash
import dash_bootstrap_components as dbc
from dash import html
from src.constant import SIDEBAR_STYLE


def sidebar() -> html.Div:
    """
    创建一个侧边栏

    Returns
    -------
    html.Div
        侧边栏
    """
    return html.Div(
        [
            html.Br(),
            html.Img(
                src="assets/logo.png", style={"width": "100%", "color": "#921235"}
            ),
            # html.H1(
            #     "兴证知己",
            #     className="text-center p-3 border border-dark",
            #     style={
            #         "color": "#921235",
            #         "font-size": "30px",
            #         "font-weight": "bold",
            #         "font-family": " 'Source Han Sans CN', sans-serif",
            #     },
            # ),
            html.Hr(),
            dbc.Nav(
                [
                    dbc.NavLink(
                        html.Div(page["name"]),
                        href=page["path"],
                        active="exact",
                    )
                    for page in dash.page_registry.values()
                ],
                pills=True,
                vertical=True,
                className="nav",
            ),
            # html.Hr(),
            # html.Img(src="assets/郑可栋.png", style={"width": "100%"}),
        ],
        style=SIDEBAR_STYLE,
    )
