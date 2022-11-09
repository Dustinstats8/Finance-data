import pandas as pd
import requests
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime
import time

# set user agent info to help with bot detectors on websites
user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " \
                 "(KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36"
headers = {'User-Agent': user_agent}

# to print full dataframe in console -
#   with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#       print(df)

# authorize script with google service account credentials
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_file('finance-central-367523-ad244a180492.json', scopes=scopes)
gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)


def yahoo_trending_tickers():
    url = "https://finance.yahoo.com/trending-tickers"
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')

    table = soup.findAll('tr')

    ticker_list = []
    name_list = []
    last_price_list = []
    change_list = []
    pChange_list = []
    for i in table:
        try:
            # For more specific elements, use a dictionary for the attributes in each column
            ticker_list.append(i.find('td', {'aria-label': 'Symbol'}).text)
            name_list.append(i.find('td', {'aria-label': 'Name'}).text)
            last_price_list.append(i.find('td', {'aria-label': 'Last Price'}).text)
            change_list.append(i.find('td', {'aria-label': 'Change'}).text)
            pChange_list.append(i.find('td', {'aria-label': '% Change'}).text)
        except AttributeError:
            print('')

    yahoo_trending_df = pd.DataFrame(list(zip(ticker_list, name_list, last_price_list, change_list, pChange_list)),
                                     columns=['Symbol', 'Company', 'Last Price', 'Change', '% Change'])

    return yahoo_trending_df


def yahoo_highest_options():
    url = 'https://finance.yahoo.com/options/highest-open-interest'
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')

    table = soup.findAll('tr')

    symbol_list = []
    underlying_symbol_list = []
    name_list = []
    strike_list = []
    exp_date_list = []
    price_list = []
    change_list = []
    pChange_list = []
    bid_list = []
    ask_list = []
    volume_list = []
    interest_list = []

    for i in table:
        try:
            symbol_list.append(i.find('td', {'aria-label': 'Symbol'}).text)
            underlying_symbol_list.append(i.find('td', {'aria-label': 'Underlying Symbol'}).text)
            name_list.append(i.find('td', {'aria-label': 'Name'}).text)
            strike_list.append(i.find('td', {'aria-label': 'Strike'}).text)
            exp_date_list.append(i.find('td', {'aria-label': 'Expiration Date'}).text)
            price_list.append(i.find('td', {'aria-label': 'Price (Intraday)'}).text)
            change_list.append(i.find('td', {'aria-label': 'Change'}).text)
            pChange_list.append(i.find('td', {'aria-label': '% Change'}).text)
            bid_list.append(i.find('td', {'aria-label': 'Bid'}).text)
            ask_list.append(i.find('td', {'aria-label': 'Ask'}).text)
            volume_list.append(i.find('td', {'aria-label': 'Volume'}).text)
            interest_list.append(i.find('td', {'aria-label': 'Open Interest'}).text)
        except AttributeError:
            print('')

    yahoo_options_df = pd.DataFrame(list(zip(symbol_list, underlying_symbol_list, name_list, strike_list, exp_date_list,
                                             price_list, change_list, pChange_list, bid_list, ask_list, volume_list,
                                             interest_list)), columns=['Symbol', 'Underlying Symbol', 'Name', 'Strike',
                                                                       'Expiration Date', 'Price', 'Change', '% Change',
                                                                       'Bid', 'Ask', 'Volume', 'Interest'])

    return yahoo_options_df


def cnn_most_active():
    # use chrome network tool to find ajax request url
    url = 'https://production.dataviz.cnn.io/markets/stocks/actives/11/2'
    cnn_most_active_stocks = requests.get(url, headers=headers)
    # convert json response data to pandas dataframe
    cnn_most_active_stocks = cnn_most_active_stocks.json()
    cnn_most_active_stocks = pd.json_normalize(cnn_most_active_stocks)
    # rename columns for better viewing
    cnn_most_active_stocks = cnn_most_active_stocks.rename({'name': 'Company', 'symbol': 'Symbol', 'current_price': 'Price',
                                'prev_close_price': 'Previous Close Price', 'price_change_from_prev_close': 'Change',
                                'percent_change_from_prev_close': '% Change', 'prev_close_date': 'Previous Close Date',
                                'sort_order_index': 'Sort Order Index', 'last_updated': 'Last Updated',
                                'mover_type': 'Mover Type', 'market_type': 'Market Type', 'market_volume': 'Market Volume',
                                'low_52_week': 'Low 52 week', 'high_52_week': 'High 52 Week', 'low_52_week_date':
                                'Low 52 Week Date', 'high_52_week_date': 'High 52 Week Date', 'current_day_price_low':
                                'Current Day Price Low', 'current_day_price_high': 'Current Day Price High',
                                'mod_symbol': 'Mod Symbol'}, axis='columns')

    return cnn_most_active_stocks


def cnn_crypto():
    url = 'https://production.dataviz.cnn.io/markets/crypto/summary/NCI-NAS,BTCUSD-BITS,ETHUSD-BITS,LTCUSD-BITS,XRPUSD-BITS'
    cnn_crypto_prices = requests.get(url, headers=headers)
    cnn_crypto_prices = cnn_crypto_prices.json()
    cnn_crypto_prices = pd.json_normalize(cnn_crypto_prices)
    cnn_crypto_prices = cnn_crypto_prices.rename({'name': 'Name', 'symbol': 'Symbol', 'current_price': 'Current Price',
                                                  'prev_close_price': 'Previous Close Price', 'price_change_from_prev_close':
                                                  'Change', 'percent_change_from_prev_close': '% Change', 'prev_close_date':
                                                  'Previous Close Date', 'sort_order_index': 'Sort Order Index',
                                                  'last_update': 'Last Updated', 'pretty_symbol': 'Pretty Symbol'})

    return cnn_crypto_prices


def build_yahoo_trending_sheet():
    gs = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wKHU8U9PaQ7C-uPHEIGTRv0KWqyqRjT-fd_2n44nhRg/edit#gid=0')
    worksheet0 = gs.worksheet('Yahoo Trending')
    set_with_dataframe(worksheet=worksheet0, dataframe=yahoo_trending_tickers(), include_index=False,
                       include_column_header=True, resize=True)


def build_yahoo_highest_options_sheet():
    gs = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wKHU8U9PaQ7C-uPHEIGTRv0KWqyqRjT-fd_2n44nhRg/edit#gid=1')
    worksheet1 = gs.worksheet('Yahoo Highest Options')
    set_with_dataframe(worksheet=worksheet1, dataframe=yahoo_highest_options(), include_index=False,
                       include_column_header=True, resize=True)


def build_cnn_trending_sheet():
    gs = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wKHU8U9PaQ7C-uPHEIGTRv0KWqyqRjT-fd_2n44nhRg/edit#gid=2')
    worksheet2 = gs.worksheet('CNN Trending')
    set_with_dataframe(worksheet=worksheet2, dataframe=cnn_most_active(), include_index=False,
                       include_column_header=True, resize=True)


def build_cnn_crypto_sheet():
    gs = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wKHU8U9PaQ7C-uPHEIGTRv0KWqyqRjT-fd_2n44nhRg/edit#gid=3')
    worksheet3 = gs.worksheet('CNN Crypto')
    set_with_dataframe(worksheet=worksheet3, dataframe=cnn_crypto(), include_index=False,
                       include_column_header=True, resize=True)


def main():
    # script will run indefinitely on repl.it
    # set times for script to execute within US market hours
    # datetime returns UTC so -5 hours for EST
    while True:
        now = datetime.now()
        start_time = now.replace(hour=14, minute=30, second=0, microsecond=0)
        end_time = now.replace(hour=21, minute=0, second=0, microsecond=0)
        while start_time < now < end_time:
            build_yahoo_trending_sheet()
            build_yahoo_highest_options_sheet()
            build_cnn_trending_sheet()
            build_cnn_crypto_sheet()
        time.sleep(10)


if __name__ == '__main__':
    main()
