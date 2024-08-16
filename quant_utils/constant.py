# !/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:WILCOXON
@file:constant.py
@time:2021/03/09
"""
import datetime
import json

from quant_utils.utils import load_json

DATE_FORMAT = "%Y%m%d"
TICK_FORMAT = "%H%M%S"
TODAY = datetime.datetime.now().strftime(DATE_FORMAT)

LOG_FORMAT = "TIME:%(asctime)s - Level:%(levelname)s - Mesaage:%(message)s"

LOG_CONFIG_PATH = "D:/config/log_config.json"
DB_CONFIG_PATH = "D:/config/db_config.json"
EMAIL_CONFIG_PATH = "D:/config/email_config.json"
BARRA_NAME_PATH = "D:/config/barra_name.json"

EMAIL_CONFIG = load_json(EMAIL_CONFIG_PATH)
DB_CONFIG = load_json(DB_CONFIG_PATH)
BARRA_SW21_FACTOR_NAME_DICT = load_json(BARRA_NAME_PATH)
LOG_FILE_PATH = load_json(LOG_CONFIG_PATH)["path"] + f"{TODAY}.log"
