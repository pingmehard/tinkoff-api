key_validate  = {'account':{'id': 'INT', 
                            'type': 'TEXT', 
                            'name': 'TEXT', 
                            'status': 'TEXT', 
                            'openedDate':'TEXT', 
                            'closedDate':'TEXT', 
                            'accessLevel': 'TEXT',
                            'deleted_flg': 'BOOL',
                            'processed_dttm':'DATETIME'},
                 'test_table':{'id': 'INT',
                            'name': 'TEXT',
                            'deleted_flg': 'BOOL',
                            'processed_dttm':'DATETIME'},
                 'portfolio_positions': {'figi': 'TEXT',
                            'instrumentType': 'TEXT',
                            'quantity': 'FLOAT',
                            'averagePositionPrice': 'FLOAT',
                            'expectedYield':'FLOAT',
                            'currentPrice':'FLOAT',
                            'deleted_flg': 'BOOL',
                            'processed_dttm':'DATETIME'},
                 'operations': {'id':'INT',
                            'parentOperationId':'INT',
                            'currency': 'TEXT', 
                            'payment':'FLOAT', 
                            'price':'FLOAT', 
                            'state': 'TEXT', 
                            'quantity':'FLOAT', 
                            'quantityRest':'FLOAT', 
                            'figi': 'TEXT', 
                            'instrumentType': 'TEXT', 
                            'date': 'TEXT', 
                            'type': 'TEXT', 
                            'operationType': 'TEXT',
                            'deleted_flg': 'BOOL',
                            'processed_dttm':'DATETIME'},
                 'broker_report': {'tradeId': 'TEXT', 
                            'orderId': 'TEXT', 
                            'figi': 'TEXT', 
                            'executeSign': 'TEXT', 
                            'tradeDatetime': 'TEXT', 
                            'exchange': 'TEXT', 
                            'classCode': 'TEXT', 
                            'direction': 'TEXT', 
                            'name': 'TEXT', 
                            'ticker': 'TEXT', 
                            'price': 'TEXT', 
                            'quantity': 'TEXT', 
                            'orderAmount': 'TEXT', 
                            'aciValue': 'TEXT', 
                            'totalOrderAmount': 'TEXT', 
                            'brokerCommission': 'TEXT', 
                            'exchangeCommission': 'TEXT', 
                            'exchangeClearingCommission': 'TEXT', 
                            'party': 'TEXT', 
                            'clearValueDate': 'TEXT', 
                            'secValueDate': 'TEXT', 
                            'brokerStatus': 'TEXT', 
                            'separateAgreementType': 'TEXT',
                            'separateAgreementNumber': 'TEXT', 
                            'separateAgreementDate': 'TEXT', 
                            'deliveryType': 'TEXT',
                            'deleted_flg': 'BOOL',
                            'processed_dttm':'DATETIME'}
                 }

primary_key = {'account': ['id'], 'test_table':['id'], 'portfolio_positions': ['figi'],'operations':['id'], 'broker_report':['tradeId']}
key_to_load = {'broker_report':['tradeDatetime']}

from datetime import datetime


def validate_case(table_name,data):
    insert_datetime = datetime. now()
    true_keys = key_validate[table_name]
    true_insert = []
    for dic in data:
        true_dic = {x: None for x in true_keys}
        for new_keys in true_keys:
            if new_keys not in dic.keys() and new_keys != 'deleted_flg' and new_keys != 'processed_dttm':
                print("ATTENTION! MISSING KEY: " + str(new_keys))
        for keys in dic:
            if keys in true_dic:
                true_dic[keys] = dic[keys]
            else:
                print("ATTENTION! NEW KEY: " + str(keys))
        true_insert.append(true_dic)
        true_dic['processed_dttm'] = insert_datetime
        
    return true_insert, true_keys, insert_datetime
