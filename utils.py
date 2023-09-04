# fix and add to jumps in datetime correct mapping for candels interval


import datetime
from dateutil.relativedelta import relativedelta
import requests



with open('tinkoff_token.txt', 'r') as f:
    token = f.read()
    f.close()

dict_currency = {
    'EUR' : 'BBG0013HJJ31',
    'USD' : 'BBG0013HGFT4'
}

par = {
    'Authorization' : f'Bearer {token}'
}


def return_datetime_delta(timestamp:str = None, interval:int = None):
    '''
    Return nowdatetime with tinkoff api datetime format string.

    jump_back_to::allows to choose how far from now function return datetime.
    It is acceptable for getting "from" date for candles. https://tinkoff.github.io/investAPI/marketdata/#candleinterval
    '''

    if timestamp and not interval:
        return timestamp
    # return now datetime in tinkoff api format
    if not interval:
        return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # this is mapping for maximum tinkoff intervals due to candles frequency
    jump_mapping = {
        1: datetime.timedelta(days=1),
        2: relativedelta(days=1) - relativedelta(minutes=5),
        3: relativedelta(days=1) - relativedelta(minutes=15),
        4: relativedelta(weeks=1) - relativedelta(hours=1),
        5: relativedelta(years=1) - relativedelta(days=1),
        13: relativedelta(years=10) - relativedelta(months=1),
    }

    if not timestamp:
        now = datetime.datetime.now() - jump_mapping[interval]
    else:
        now = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") - jump_mapping[interval]

    return now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def return_currency(currency = 'USD'):

    '''
    Return last actual value of currency in rubles. By default converts USD to RUB.
    Options: USD, EUR
    '''
    if currency == 'RUB':
        
        return 1
        
    else:

        json = {
            'figi' : [dict_currency[currency]]
        }

        results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices', headers = par, json = json)

        price = results.json()['lastPrices'][0]['price']

        lastprice = int(price['units']) + float(price['nano']) / 1_000_000_000

        return lastprice