from validate import key_validate, primary_key
from my_sql_connector import DBInvest
import json


with open('my_access.json') as f:
    access = json.load(f)
connection = DBInvest.create_connection(host_name = access["host"],port_name = access["port"],user_name = access["user"],password_name = access["password"])


def first_creation(connection, schema, tables, primary_keys):
    '''!  If tables are already exist, there will be no changes in it'''
    for table in tables:
        primary_key = primary_keys[table]
        columns = tables[table]
        DBInvest.create_query(connection, schema, table, columns, primary_key)
    return 'All tables were created successfully'

first_creation(connection, 'test', key_validate, primary_key)

