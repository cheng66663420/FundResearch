import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime, date
from time import strftime, sleep
from bs4 import BeautifulSoup as bs
import requests
HEADERS = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})


# helper funcs
def clean_header(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

def get_now():
    now = datetime.now().strftime('%Y-%m-%d_%Hh%Mm')
    return now

def get_finviz_screener(screener_name, tickers_list):
    """
    :param screener_name: the screener is the url for the screener you want. One only!
    :param tickers_list: simple list with all the tickers to be queried

    :return: dataframe with the results of the screener

    Valid names are OVERVIEW, VALUATION, FINANCIAL, OWNERSHIP, PERFORMANCE, TECHNICALS, CHARTS, NEWS
    
    """
    
    # calculates how many pages the screener will have
    p = len(tickers_list)//20+1
    
    # creating the strings for each screener
    tickers_string = ','.join(map(str, tickers_list))
    if screener_name == 'OVERVIEW':
        screener_string = 'https://finviz.com/screener.ashx?v=111&t=' + tickers_string + '&r='
    elif screener_name == 'VALUATION':
        screener_string = 'https://finviz.com/screener.ashx?v=121&t=' + tickers_string + '&r='
    elif screener_name == 'FINANCIAL':
        screener_string = 'https://finviz.com/screener.ashx?v=161&t=' + tickers_string + '&r='
    elif screener_name == 'OWNERSHIP':
        screener_string = 'https://finviz.com/screener.ashx?v=131&t=' + tickers_string + '&r='
    elif screener_name == 'PERFORMANCE':
        screener_string = 'https://finviz.com/screener.ashx?v=141&t=' + tickers_string + '&r='
    elif screener_name == 'TECHNICALS':
        screener_string = 'https://finviz.com/screener.ashx?v=171&t=' + tickers_string + '&r='
    elif screener_name == 'CHARTS':
        screener_string = 'https://finviz.com/screener.ashx?v=211&t=' + tickers_string + '&r='
    elif screener_name == 'NEWS':
        screener_string = 'https://finviz.com/screener.ashx?v=321&t=' + tickers_string + '&r='
    else:
        return ('Valid names are [OVERVIEW, VALUATION, FINANCIAL, OWNERSHIP, PERFORMANCE, TECHNICALS, CHARTS, NEWS].')
    
    l=[]
    pages = list(range(0,p))
    for page in pages:
        url = screener_string + str(page*2) + '1'
        soup = bs(requests.get(url, headers=HEADERS).content, features="lxml")

        table_rows = soup.select('#screener-content table')[3].find_all('tr')
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            l.append(row)

        sleep(2)
        print('#### took care of page ' + str(page+1) + ' out of ' + str(p))

    finviz_result = pd.DataFrame(l)
    finviz_result.columns = list(finviz_result.loc[0])
    finviz_result = finviz_result[~finviz_result.duplicated()][1:]
    return finviz_result

def get_biz_data_all(tickers_list):
    """
    :param tickers_list: simple list with all the tickers to be queried
    
    :return: dataframe with all the metrics and info for all the tickers

    This function goes through all the screener pages available and scrapes the information.
    It may take a minute or two if you have more than 40 stocks in the list.

    Consider using the get_finviz_screener function if you want specific information.

    """

    # creating the strings for each screener
    tickers_string = ','.join(map(str, tickers_list))


    # calculates how many pages the screener will have
    p = len(tickers_list)//20+1
    print(f"There are {p} pages")
    
    # SCRAPE TIME! Go get some coffee!
    print('####\nOverview page scrape in progress...')
    finviz_overview_df = get_finviz_screener('OVERVIEW', tickers_list)
    print('####\nPerformance page scrape in progress...')
    finviz_perf_df = get_finviz_screener('PERFORMANCE', tickers_list)
    print('####\nTechnical page scrape in progress...')
    finviz_tech_df = get_finviz_screener('TECHNICALS', tickers_list)
    print('####\nOwnership page scrape in progress...')
    finviz_owner_df = get_finviz_screener('OWNERSHIP', tickers_list)
    print('####\nValuation page scrape in progress...')
    finviz_value_df = get_finviz_screener('VALUATION', tickers_list)
    print('####\nFinancial page scrape in progress...')
    finviz_finance_df = get_finviz_screener('FINANCIAL', tickers_list)
    print('####\nMerging everything together...')
    finviz_merged_raw = pd.merge(finviz_overview_df,finviz_perf_df, on='Ticker')
    finviz_merged_raw = pd.merge(finviz_merged_raw,finviz_tech_df, on='Ticker')
    finviz_merged_raw = pd.merge(finviz_merged_raw,finviz_owner_df, on='Ticker')
    finviz_merged_raw = pd.merge(finviz_merged_raw,finviz_value_df, on='Ticker')
    finviz_merged_raw = pd.merge(finviz_merged_raw,finviz_finance_df, on='Ticker')

    # there will be lots of repeated column names, so we need to clean it... it's not elegant, but it's working!
    cols_clean = ['Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap_x', 'P/E_x', 'Price_x',
                  'Change_x', 'Volume_x', 'Perf Week', 'Perf Month', 'Perf Quart', 'Perf Half', 'Perf Year', 'Perf YTD',
                  'Volatility W', 'Volatility M', 'Recom', 'Avg Volume_x', 'Rel Volume', 'Beta', 'ATR',
                  'SMA20', 'SMA50', 'SMA200', '52W High', '52W Low', 'RSI', 'from Open', 'Gap',
                  'Outstanding', 'Float', 'Insider Own', 'Insider Trans', 'Inst Own', 'Inst Trans',
                  'Float Short', 'Short Ratio', 'Fwd P/E', 'PEG', 'P/S', 'P/B', 'P/C', 'P/FCF',
                  'EPS this Y', 'EPS next Y', 'EPS past 5Y', 'EPS next 5Y', 'Sales past 5Y', 'Dividend',
                  'ROA', 'ROE', 'ROI', 'Curr R', 'Quick R', 'LTDebt/Eq', 'Debt/Eq',
                  'Gross M', 'Oper M', 'Profit M', 'Earnings']

    finviz_merged_clean = finviz_merged_raw[cols_clean]

    finviz_merged_clean = finviz_merged_clean.iloc[:,[0, 1, 2, 3, 4, 5, 7, 8, 11, 14, 17, 18, 19, 20, 21, 22,
                                                      23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
                                                      37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
                                                      51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
                                                      65, 66, 67, 68]]

    finviz_merged_clean.columns = ['Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap',
                                   'P/E', 'Price', 'Change', 'Volume', 'Perf Week', 'Perf Month',
                                   'Perf Quart', 'Perf Half', 'Perf Year', 'Perf YTD', 'Volatility W',
                                   'Volatility M', 'Recom', 'Avg Volume', 'Rel Volume', 'Beta', 'ATR',
                                   'SMA20', 'SMA50', 'SMA200', '52W High', '52W Low', 'RSI', 'from Open',
                                   'Gap', 'Outstanding', 'Float', 'Insider Own', 'Insider Trans',
                                   'Inst Own', 'Inst Trans', 'Float Short', 'Short Ratio', 'Fwd P/E',
                                   'PEG', 'P/S', 'P/B', 'P/C', 'P/FCF', 'EPS this Y', 'EPS next Y',
                                   'EPS past 5Y', 'EPS next 5Y', 'Sales past 5Y', 'Dividend', 'ROA', 'ROE',
                                   'ROI', 'Curr R', 'Quick R', 'LTDebt/Eq', 'Debt/Eq', 'Gross M', 'Oper M',
                                   'Profit M', 'Earnings']

    # finviz_merged_clean.to_excel('outputs/finviz_stats/finviz_screener_filtered_{}.xlsx'.format(get_now()), index=False)
    # finviz_merged_clean.to_csv('finviz_screener_filtered.csv', index=False)

    print('ALL DONE!')
    return finviz_merged_clean

