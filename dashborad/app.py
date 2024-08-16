import dash
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html
from src.components.side_bar import sidebar
from src.constant import APP_CONFIG


def main(debug: bool = True, host: str = "0.0.0.0", port: int = 8080):
    app = Dash(
        __name__,
        use_pages=True,
    )

    app.layout = dbc.Container(
        [
            dbc.Col([sidebar()]),
            dbc.Col([dash.page_container]),
        ],
        fluid=True,
    )
    app.run(debug=debug, host=host, port=port)


if __name__ == "__main__":
    main(debug=APP_CONFIG["debug"], host=APP_CONFIG["host"], port=APP_CONFIG["port"])
