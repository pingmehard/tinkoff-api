from os import getgrouplist
import mysql.connector
from validate import validate_case, primary_key

class DBInvest:
    def create_connection(host_name, port_name, user_name, password_name):
        '''Create connection to DB'''
        cnx = None
        try:
            cnx = mysql.connector.connect(
            host=host_name,
            port=port_name,
            user=user_name,
            password=password_name
            )
            print('Connection to DB successful')
        except OSError as e:
            print(f"The error '{e}' occurred")
        return cnx

    def select_query(connection, shema, table_name, columns = '*'):
        '''Execute SELECT'. Columns must be the list of necessary columns.
            Returnts the list of turples [(*,*),(*,*)].
            If you want specific column - type it as str. You will get the list turple with 1 atribute [(*,),(*,)].
            If you want more then 1 column - type it as list []'''

        if type(columns) is str and columns != '*':
                columns = columns
        elif columns != '*':
                columns = ", ".join(columns)
        else: 
                columns = '*'

        query = 'Select ' \
                + columns \
                + ' from ' \
                + shema \
                + '.' \
                + table_name \

        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except OSError as e:
            print(f"The error '{e}' occurred")
    
    def approve_schema(connection, shema):
        '''To create shema if it doesn't exist in DB'''
        query = 'CREATE SCHEMA if not exists `'\
                + shema \
                + '`;'
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            connection.commit()
        except OSError as e:
            print(f"The error '{e}' occurred")

    def create_query(connection, shema, table_name, columns, primary_key):
        '''Execute CREATE TABLE
        connection - obviously, 
        shema - DB shema (str), 
        table_name - (str), 
        columns - dict with key = name of column, value = type of column (SQL standart like 'TEXT', 'INT', 'DATETIME itc), 
        primary_keys - (list), even if the table has 1 key (пока что без них - чтобы делать insert)
        '''
        query = ''
        list_of_atr = []
        for k,v in columns.items():
                list_of_atr.append(str(k)+' '+str(v))

        DBInvest.approve_schema(connection, shema)

        query = 'CREATE TABLE IF NOT EXISTS ' \
                + shema \
                + '.' \
                + table_name \
                + ' (' \
                + ", ".join(list_of_atr) \
                + ') ENGINE = InnoDB'

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            connection.commit()
            print("Query executed successfully. Table was created")
        except OSError as e:
            print(f"The error '{e}' occurred")

        return 'The table was created successfully'


    def insert_query(connection, shema, table_name, true_keys, data):
        query = 'INSERT INTO ' \
                + shema \
                + '.' \
                + table_name \
                + ' (' \
                + ", ".join(true_keys) \
                + ') VALUES (' \
                + ", ".join('%s' for i in range(len(true_keys))) \
                + ')'
        insert_data = [tuple(data[i].values()) for i in range(len(data))]
        
        cursor = connection.cursor()
        cursor.executemany(query, insert_data)
        connection.commit()

        print('All data have been inserted')