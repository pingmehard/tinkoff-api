from multiprocessing import connection
from my_sql_connector import DBInvest
from validate import primary_key, validate_case, key_to_load
from utils import return_now_datetime
import datetime

from_time_to_load = str(datetime.datetime.date(datetime.datetime.now())) + 'T00:00:00.000Z'

class Insert:
    def insert_delete(connection, shema, table_name, data):
        true_insert,true_keys, insert_datetime = validate_case(table_name,data) #list of dict
        exist = DBInvest.insert_query(connection,shema, table_name, true_keys, true_insert) #list of turples

        delete_query = 'DELETE FROM ' \
                        + shema \
                        + '.' \
                        + table_name \
                        + ' WHERE ' \
                        + 'cast(processed_dttm as datetime) < ' \
                        + 'cast(\'' \
                        + str(insert_datetime) \
                        + '\' as datetime)'

        cursor = connection.cursor()
        cursor.execute(delete_query)
        connection.commit()

        cursor.close()
        connection.close()

        print('All old data have been deleted')

        return true_insert,exist

    def insert_only(connection, shema, table_name, data): 
        # time_to_load - expect from what time to load data
        true_insert,true_keys, insert_datetime = validate_case(table_name,data) #list of dict
        exist = DBInvest.insert_query(connection,shema, table_name, true_keys, true_insert) #list of turples
        
        return true_insert,exist