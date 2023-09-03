from insert import Insert
import schedule
import main
import datetime
from my_sql_connector import DBInvest
import json
import time

def account_insert():

    with open('my_access.json') as f:
        access = json.load(f)
    connection = DBInvest.create_connection(host_name = access["host"],port_name = access["port"],user_name = access["user"],password_name = access["password"])

    for_account = main.UsersService()
    all_acount = for_account.get_accounts()

    try:
        Insert.insert_delete(connection, 'test', 'account', all_acount)
        print('Accounts have been inserted successfully by schedule.')
    except OSError as e:
        print(f"The error '{e}' occurred")
        
    connection.close()


def insert_all_operations():

    with open('my_access.json') as f:
        access = json.load(f)
    connection = DBInvest.create_connection(host_name = access["host"],port_name = access["port"],user_name = access["user"],password_name = access["password"])
    
    from_time_to_load = str(datetime.datetime.date(datetime.datetime.now())) + 'T00:00:00.000Z'
    for_br = main.OperationsService()
    for_account = main.UsersService()
    all_acount = for_account.get_accounts()

    for dic in all_acount:
        account_id = str(dic['id'])
        br_data = for_br.get_broker_report(accountId = account_id, from_ = from_time_to_load)
        time.sleep(1)
        if br_data == None:
            continue
        else:
            Insert.insert_only(connection, 'test', 'broker_report', br_data)
    print('Operations have been inserted successfully by schedule.')

    connection.close()



schedule.every().day.at("01:00").do(account_insert)
schedule.every().day.at("01:00").do(insert_all_operations)

while True:
    schedule.run_pending()
    time.sleep(1)

