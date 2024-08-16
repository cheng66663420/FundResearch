import quant_utils.data_moudle as dm
from quant_utils.constant import TODAY

LAST_TRADE_DT = (
    dm.get_recent_trade_dt(TODAY)
    if dm.if_trade_dt(TODAY)
    else dm.offset_trade_dt(TODAY, 0)
)
