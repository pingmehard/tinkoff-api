import requests
import datetime
import time
import sys

from utils import return_now_datetime
from utils import return_currency


with open('tinkoff_token.txt', 'r') as f:
    token = f.read()
    f.close()

nano_delimiter = 1_000_000_000
delta = datetime.timedelta(days=30)

par = {
    "Authorization" : f'Bearer {token}',
    "Content-Type": "application/json",
    "accept" : "application/json"
}

def error_wait(api_link, json_task_id):
    
    req = requests.post(api_link, headers = par, json = json_task_id)

    if 'x-envoy-ratelimited' in req.headers:
        print('Превышен лимит запросов, данные загрузятся через ',req.headers['x-ratelimit-reset'],'секунд')
        time.sleep(int(req.headers['x-ratelimit-reset']))
        req = requests.post(api_link, headers = par, json = json_task_id)
        return req.json()
    else:
        return req.json()

class UsersService:

    def get_accounts(self):
        '''
        Return full of copy your Tinkoff.Investments accounts json.
        '''
        results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts', headers = par, json = {})

        return results.json()['accounts']
    
    def get_accounts_main_values(self):
        '''
        Return dict ids and names of accounts in Tinkoff.Investments.
        '''
        results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts', headers = par, json = {})

        return {i['id']:i['name'] for i in results.json()['accounts']}

class OperationsService:

    def get_broker_report(self, accountId, from_ = '', to = return_now_datetime()):
        '''
        Generate Broker report with all details of your operations and portfolio for a period. By default it is 30 days. Define "to" datetime and it calculates "to" datetime - 30 days.
        "30 days" is a default lambda of this method.
        Requires:
        accountId - Id of accounts in tinkoff.Investments. UsersService.get_account method returns a json with accountId.
        from_ - datetime, which you should state for datetime window. Default ["to" datetime] minus [30 days]. 
        to - datetime, which you should state for datetime window. By default datetime when the class is initialized. Ex: "2022-03-30T19:00:54.870Z".

        '''

        get_broker_report_link = 'https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.OperationsService/GetBrokerReport'

        if from_ == '':
            from_ = (datetime.datetime.strptime(to, "%Y-%m-%dT%H:%M:%S.%fZ") - delta).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        json_task_id = {
            "generateBrokerReportRequest": {
                "accountId": accountId,
                "from": from_,
                "to": to
                }
        }

        results = error_wait(get_broker_report_link, json_task_id)

        if 'generateBrokerReportResponse' in results:
            if 'taskId' in results['generateBrokerReportResponse']:
                taskId = results['generateBrokerReportResponse']['taskId']
                time.sleep(10)
                json = {
                "generateBrokerReportRequest": {
                    "accountId": accountId,
                    "from": from_,
                    "to": to
                    },
                "getBrokerReportRequest": {
                    "taskId": taskId,
                    "page": 0
                }
                }
                print(1)
                results = error_wait(get_broker_report_link, json)
                if 'getBrokerReportResponse' in results:
                    brokerReport = results['getBrokerReportResponse']['brokerReport']
                elif results['message'] == None:
                    return None
                    
        elif 'getBrokerReportResponse' in results:
            if 'brokerReport' in results['getBrokerReportResponse']:
                print(2)
                brokerReport = results['getBrokerReportResponse']['brokerReport']
        elif results['message'] == None:
            print(3)
            return None
        else:
            try:
                print(4)
                time.sleep(5)
                results = error_wait(get_broker_report_link, json_task_id)
                brokerReport = results['getBrokerReportResponse']['brokerReport']
            except:
                print(5)
                return None

        i = 0
        for item in brokerReport:
            price = return_currency(item['orderAmount']['currency'].upper()) * (float(item['price']['units']) + float(item['price']['nano']) / nano_delimiter)
            brokerReport[i]['price'] = price

            orderAmount = return_currency(item['orderAmount']['currency'].upper()) * (float(item['orderAmount']['units']) + float(item['orderAmount']['nano']) / nano_delimiter)
            brokerReport[i]['orderAmount'] = orderAmount

            aciValue = float(item['aciValue']['units']) + float(item['aciValue']['nano']) / nano_delimiter
            brokerReport[i]['aciValue'] = aciValue

            totalOrderAmount = return_currency(item['totalOrderAmount']['currency'].upper()) * (float(item['totalOrderAmount']['units']) + float(item['totalOrderAmount']['nano']) / nano_delimiter)
            brokerReport[i]['totalOrderAmount'] = totalOrderAmount

            brokerCommission = return_currency(item['brokerCommission']['currency'].upper()) * (float(item['brokerCommission']['units']) + float(item['brokerCommission']['nano']) / nano_delimiter)
            brokerReport[i]['brokerCommission'] = brokerCommission

            exchangeCommission = return_currency(item['exchangeCommission']['currency'].upper()) * (float(item['exchangeCommission']['units']) + float(item['exchangeCommission']['nano']) / nano_delimiter)
            brokerReport[i]['exchangeCommission'] = exchangeCommission

            exchangeClearingCommission = return_currency(item['exchangeClearingCommission']['currency'].upper()) * (float(item['exchangeClearingCommission']['units']) + float(item['exchangeClearingCommission']['nano']) / nano_delimiter)
            brokerReport[i]['exchangeClearingCommission'] = exchangeClearingCommission

            i += 1

        return brokerReport

    def get_operations(self, accountId, figi, state = "ALL", from_ = "1970-01-01T01:00:00.000Z", to = return_now_datetime()): 
        
        '''
        accountId - Id of accounts in tinkoff.Investments. UsersService.get_account method returns a json with accountId.
        from_ - datetime, which you should state for datetime window. Default "1970-01-01T01:00:00.000Z". Ex: "2022-03-01T19:00:54.870Z"
        to - datetime, which you should state for datetime window. By default datetime when the class is initialized. Ex: "2022-03-30T19:00:54.870Z".
        figi - figi of any ticket (stock) in market. The list you can get by method !!!
        state - Status of requested transactions. Defualt "ALL". Options: OPERATION_STATE_UNSPECIFIED, OPERATION_STATE_EXECUTED, OPERATION_STATE_CANCELED. See more on the https://tinkoff.github.io/investAPI/operations/#operationstate
        '''
        json = {
            "accountId": accountId,
            "from": from_,
            "to": to,
            "state": state,
            "figi": figi
            }

        states = ['OPERATION_STATE_UNSPECIFIED','OPERATION_STATE_EXECUTED', 'OPERATION_STATE_CANCELED']

        all_operations = []

        if state == 'ALL':

            for state in states:
                json = {
                    "accountId": accountId,
                    "from": from_,
                    "to": to,
                    "state": state,
                    "figi": figi
                }
                results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.OperationsService/GetOperations', headers = par, json = json)

                operations = results.json()['operations']

                all_operations.extend(operations)

        else:

            results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.OperationsService/GetOperations', headers = par, json = json)

            all_operations = results.json()['operations']

        i = 0
        for operation in all_operations:
            
            payment = return_currency(operation['payment']['currency'].upper()) * (float(operation['payment']['units']) + float(operation['payment']['nano']) / nano_delimiter)
            all_operations[i]['payment'] = payment

            price = return_currency(operation['price']['currency'].upper()) * (float(operation['price']['units']) + float(operation['price']['nano']) / nano_delimiter)
            all_operations[i]['price'] = price

            if operation['trades'] != []:
                trades = operation['trades']

                for trade in trades:
                    trade['price'] = return_currency(trade['price']['currency'].upper()) * (float(trade['price']['units']) + float(trade['price']['nano']) / nano_delimiter)

                operation['trades'] = trades

            operation['currency'] = 'RUB'

            i += 1

        return all_operations

    def get_portfolio(self, accountId):
        '''
        Get all raw your portfolio data. Required accountId from UsersService.get_account method.
        '''
        json = {
            "accountId": accountId
        }

        results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.OperationsService/GetPortfolio', headers = par, json = json)

        return results.json()

    def get_portfolio_positions(self, accountId):
        '''
        Get your portfolio positions. It contains all your positions in account with FIGI, quantity, prices and other moments.
        Also it contains prepared data dict for using it in table classes like a pandas DataFrame.
        Required accountId from UsersService.get_account method.
        '''
        json = {
            "accountId": accountId
        }

        results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.OperationsService/GetPortfolio', headers = par, json = json)
        positions = results.json()['positions']

        i = 0

        for inst in positions:
    
            quantity = float(inst['quantity']['units']) + float(inst['quantity']['nano']) / nano_delimiter
            positions[i]['quantity'] = quantity
            
            averagePositionPrice = return_currency(inst['averagePositionPrice']['currency'].upper()) * (float(inst['averagePositionPrice']['units']) + float(inst['averagePositionPrice']['nano']) / nano_delimiter)
            positions[i]['averagePositionPrice'] = averagePositionPrice

            expectedYield = return_currency(inst['currentPrice']['currency'].upper()) * (float(inst['expectedYield']['units']) + float(inst['expectedYield']['nano']) / nano_delimiter)
            positions[i]['expectedYield'] = expectedYield

            currentPrice = return_currency(inst['currentPrice']['currency'].upper()) * (float(inst['currentPrice']['units']) + float(inst['currentPrice']['nano']) / nano_delimiter)
            positions[i]['currentPrice'] = currentPrice
            
            del positions[i]['currentNkd'], positions[i]['averagePositionPriceFifo'], positions[i]['quantityLots']

            i += 1

        return positions

class InstrumentsService:

    def get_dividends(self, figi, from_ = "1970-01-01T01:00:00.000Z", to = return_now_datetime()):
        '''
        Get dividends information from Tinkoff.Investments. Contains list of jsons for earch dividentd payment. Future divs are included.
        Expects figi (for example from get_portfolio_positions method), from and to. 

        figi is like "BBG004HXD0G8"
        from_ is like "1970-01-01T01:00:00.000Z". By default is "1970-01-01T01:00:00.000Z"
        to is like "1970-01-01T01:00:00.000Z". By default is current datetime when the class is initialized. For support and actual data update use your own function.
        '''

        json = {
            "from": from_,
            "to": to,
            "figi": figi
            }

        results = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetDividends', headers = par, json = json)

        dividends = results.json()['dividends']
        
        i = 0

        for payment in dividends:

            dividendNet = return_currency(payment['dividendNet']['currency'].upper()) * (float(payment['dividendNet']['units']) + float(payment['dividendNet']['nano']) / nano_delimiter)
            dividends[i]['dividendNet'] = dividendNet

            closePrice = return_currency(payment['closePrice']['currency'].upper()) * (float(payment['closePrice']['units']) + float(payment['closePrice']['nano']) / nano_delimiter)
            dividends[i]['closePrice'] = closePrice

            yieldValue = float(payment['yieldValue']['units']) + float(payment['yieldValue']['nano']) / nano_delimiter
            dividends[i]['yieldValue'] = yieldValue

            paymentDate = payment['paymentDate'][:10]
            dividends[i]['paymentDate'] = paymentDate

            declaredDate = payment['declaredDate'][:10]
            dividends[i]['declaredDate'] = declaredDate

            lastBuyDate = payment['lastBuyDate'][:10]
            dividends[i]['lastBuyDate'] = lastBuyDate

            recordDate = payment['recordDate'][:10]
            dividends[i]['recordDate'] = recordDate

            i += 1

        return dividends
    


class MarketDataService:


    def __init__(self) -> None:
        self.link = 'https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/'


    def get_candles(self, from_:str = None, instrument_id:str, interval:int=13):
        '''

        interval::https://tinkoff.github.io/investAPI/marketdata/#candleinterval
        instrument_id::ID of instrument, for example FIGI
        
        '''

        # нужно получить даты с и по в интервале
        # нужно получить свечи в этом интервале
        # получение нужно разбить на несколько дат от и до
        # на основе интервалов от текущего момента до "от"
        # в цикле нужно пройти от сегодня до финала
        # пока не получим пустые свечи или ошибку выдачи
        # в каждой новой итерации меняется от и до местами,
        # а от - максимальный интервальный промежуток для текущего интервала

        date_from = return_now_datetime(jump_back_to=interval)
        date_to = return_now_datetime()

        if not from_:
        
            json = {
                "from": date_from,
                "to": date_to,
                "interval": interval,
                "instrument_id": instrument_id,
            }

            results = requests.post(
                self.link + 'GetCandles',
                headers = par,
                json = json,
                timeout=10
                )
        
            print(f"candles for {json['instrument_id']} from {json['from']} to {json['to']}")

            return results.json()
        
        else:
            


    