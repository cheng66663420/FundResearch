import pandas as pd
import plotly.express as px

import data_functions.fund_data as fd
import quant_utils.data_moudle as dm
from dash import Dash, Input, Output, State, callback, dcc, html
from quant_utils.constant_varialbles import LAST_TRADE_DT

external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    [
        html.Div(
            [
                dcc.Input(id="input-1-state", type="text", value="110011"),
                dcc.Input(id="input-2-state", type="text", value="000300"),
            ]
        ),
        dcc.DatePickerRange(
            id="my-date-picker-range",
            min_date_allowed=pd.to_datetime("2000-01-01"),
            max_date_allowed=pd.to_datetime(LAST_TRADE_DT),
            start_date=pd.to_datetime("2020-01-01"),
            end_date=pd.to_datetime(LAST_TRADE_DT),
            display_format="YYYY-MM-DD",
            number_of_months_shown=3,
        ),
        html.Div(
            [
                html.Button(id="submit-button-state", n_clicks=0, children="Submit"),
            ]
        ),
        dcc.Graph(id="fund_alpha"),
    ]
)


@callback(
    Output("fund_alpha", "figure"),
    Input("submit-button-state", "n_clicks"),
    State("input-1-state", "value"),
    State("input-2-state", "value"),
    State("my-date-picker-range", "start_date"),
    State("my-date-picker-range", "end_date"),
)
def update_output(_, input1, input2, start_date, end_date):

    fund_alpha = fd.get_fund_alpha_to_index(
        ticker_symbol=input1,
        index_code=input2,
        start_date=start_date,
        end_date=end_date,
    )
    fund_alpha = fund_alpha.dropna()
    fund_alpha["END_DATE"] = pd.to_datetime(fund_alpha["END_DATE"])
    fig = px.line(
        fund_alpha,
        x="END_DATE",
        y=["SUM_FUND_RET", "SUM_INDEX_RET", "SUM_ALPHA_RET"],
    )
    fig.update_xaxes(rangeslider_visible=True)
    fig.update_layout(
        title=f"{input1} to {input2} Alpha", xaxis_title="Date", yaxis_title="Return"
    )
    return fig


if __name__ == "__main__":
    app.run(debug=True)
