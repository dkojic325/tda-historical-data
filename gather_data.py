from tda import auth, client
from tda.auth import easy_client

import json
import pandas as pd
import mysql.connector
from datetime import datetime
import pytz
import time
import asyncio

# set configure the api
token_path = '/path/to/token.pickle'
api_key = 'YOUR_API_KEY@AMER.OAUTHAP'
redirect_uri = 'https://localhost'

# database configuration
config = {
    'user': 'MySQL USERNAME',
    'password': 'PASSWORD',
    'host': '127.0.0.1',
    'database': 'DATABASE_NAME'
}

# create the cursor object
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

#token_path = '/path/to/token.pickle'
#api_key = 'YOUR_API_KEY@AMER.OAUTHAP'
#redirect_uri = 'https://your.redirecturi.com'


async def read_stream(symbols):

    async def parse_data(stock):
        try:
            response = await c.get_price_history(stock,
                    period_type=client.Client.PriceHistory.PeriodType.MONTH,
                    period=client.Client.PriceHistory.Period.ONE_MONTH,
                    frequency_type=client.Client.PriceHistory.FrequencyType.DAILY,
                    frequency=client.Client.PriceHistory.Frequency.DAILY)
            assert response.status_code == 200, response.raise_for_status()
        except Exception as e:
            print(stock)
            print(index)
            return False

        json_string = json.dumps(response.json(), indent=4)
        response_dict = json.loads(json_string)

        month_tickers = []
        symbol = response_dict["symbol"]
        is_empty = response_dict["empty"]   

        for candle in response_dict["candles"]:
            temp_dict = {}
            temp_dict["symbol"] = symbol

            date_time = datetime.fromtimestamp(
                candle['datetime'] / 1000,
                tz=pytz.timezone('US/Eastern'))
            date_string = '{0:04d}/{1:02d}/{2:02d}'.format(
                date_time.year, date_time.month, date_time.day)
            temp_dict["date"] = date_string

            temp_dict["open_price"] = candle["open"]
            temp_dict["high_price"] = candle["high"]
            temp_dict["low_price"] = candle["low"]
            temp_dict["close_price"] = candle["close"]
            temp_dict["total_volume_day_market"] = candle["volume"]

            month_tickers.append(temp_dict)
        
        sql = '''
                INSERT INTO `{}` 
                ( `symbol`, `date`, `open_price`, `high_price`, `low_price`, `close_price`, 
                `total_volume_day_market`, `total_volume_day` ) \
                VALUES ( %(symbol)s, %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, 
                %(total_volume_day_market)s, %(total_volume_day_market)s ) \
            '''.format('ticker_volume_by_day')

        cursor.executemany(sql, list(month_tickers))
        cnx.commit()

        return True

    # create steam client
    try:
        c = easy_client(
            api_key=api_key,
            redirect_uri=redirect_uri,
            token_path=token_path,
            asyncio=True)
    except FileNotFoundError:
        from selenium import webdriver
        with webdriver.Chrome(executable_path='chromedriver') as driver:
            c = auth.client_from_login_flow(
                driver, api_key, redirect_uri, token_path)

    # loop through the selected stocks and receive data
    missing_stocks = []
    for index, stock in enumerate(symbols):
        if index % 250 == 0 and index != 0:
            print(index)
            time.sleep(30)

        success = await parse_data(stock)
        if not success:
            missing_stocks.append(stock)

    print("unprocessed stocks\n")
    print(missing_stocks)

    print("completing missed stocks")
    while len(missing_stocks) > 0:
        time.sleep(5)
        print(missing_stocks[0])
        success = await parse_data(missing_stocks[0])
        if success:
            missing_stocks.pop(0)
        
    print("All stocks added successfully")

if __name__ == '__main__':

    cursor.execute('''CREATE TABLE IF NOT EXISTS ticker_volume_by_day
        (symbol varchar(255), date varchar(50), open_price float, high_price float, low_price float, close_price float, 
        total_volume_day_premarket int, total_volume_day_market int, total_volume_day_postmarket int, total_volume_day int)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS ticker_average_by_minute
        (symbol varchar(255), last_date varchar(50), latest_close float, average_volume_premarket int, 
        average_volume_market int, average_volume_postmarket int)''')

    # make the list of stocks
    symbols = pd.read_csv('stocks.csv')
    print(symbols['Symbol'].nunique())
    # start the receving streaming
    asyncio.run(read_stream(symbols['Symbol']))
