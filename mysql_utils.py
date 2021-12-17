import pandas as pd
import datetime
from datetime import timedelta
import dateutil
import pymysql as pms

import signal   # For killing scripts after a time limit
import traceback    # Debug

def insert(config, table_name, df, debug=False):
    assert(type(config) == dict)
    assert(type(table_name) == str)
    # Connect to SQL
    connection = pms.connect(host = config['host'],
                            user = config['user'],
                            password = config['password'],
                            db = config['db'])
    cursor=connection.cursor()

    # Check if given table exists
    sql = 'SHOW TABLES LIKE \'' + table_name + '\''
    cursor.execute(sql)
    data = cursor.fetchall()

    if len(data) == 0:  # Create table if it doesn't already exist
        attributes = df.columns
        joined_str = ' VARCHAR(500), '.join(attributes) + ' VARCHAR(500)'
        sql = 'CREATE TABLE ' + table_name + '(ID int NOT NULL AUTO_INCREMENT, ' + joined_str + ', PRIMARY KEY (ID) )'
        if 'id' in attributes or 'Id' in attributes or 'iD' in attributes or 'ID' in attributes:
            sql = 'CREATE TABLE ' + table_name + '(RowID int NOT NULL AUTO_INCREMENT, ' + joined_str + ', PRIMARY KEY (RowID) )'
        cursor.execute(sql)
        connection.commit()

    def _get_columns(cursor, table):    # Retrieve columns from table and add columns from df that are missing from the SQL table
        sql = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{config['db']}'
                AND TABLE_NAME = '{table}';"""
        cursor.execute(sql)
        data = cursor.fetchall()

        columns = []
        for row in data:
            for item in row:
                columns.append(item)
        # connection.close()
        return columns

    table_cols = _get_columns(cursor, table_name)

    newColumns = []
    for col in df.columns:      # Edit columns in the df so that they match MySQL convention
        col = col.replace('.','_')
        newColumns.append(col)
    df.columns = newColumns             # Replace '.' with '_' in all column names
    df.fillna(value='', inplace=True)   # Replace NaN values with '' as MySQL is not compatible with NaN
    df = df.astype(str)

    for col in df.columns:
        if col not in table_cols:
            sql = f""" ALTER TABLE {table_name} ADD COLUMN {col} VARCHAR(500)"""  # VARCHAR(MAX) is not compatible with MySQL. Max is 65535.
            print(sql)
            cursor.execute(sql)
            connection.commit()

    if debug:
        # Creating column list for insertion
        cols = "`,`".join([str(i) for i in df.columns.tolist()])

        # Iterate over each row in df and insert
        len_df = len(df)    # Debug
        ctr = 1     # Debug
        for i,row in df.iterrows():
            print('Inserting into %s: %d/%d' % (table_name, ctr, len_df))   # Debug
            ctr += 1    # Debug
            sql = "INSERT INTO `" + table_name + "` (`" + cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            # print(sql)
            cursor.execute(sql, tuple(row))
            connection.commit()
    else:
        # Creating column list for insertion
        cols = "`,`".join([str(i) for i in df.columns.tolist()])
        print('Inserting %d rows into %s' % (len(df), table_name))
        # Iterate over each row in df and insert
        for i,row in df.iterrows():
            sql = "INSERT INTO `" + table_name + "` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))
            connection.commit()
    # Close connection
    connection.close()

def delete(config, cond_list, table_name):
    assert(type(config) == dict)
    assert(type(table_name) == str)
    assert(type(cond_list) == list)
    # Connect to SQL
    connection = pms.connect(host = config['host'],
                            user = config['user'],
                            password = config['password'],
                            db = config['db'])
    cursor=connection.cursor()
    cond_list_joined = ' AND '.join(cond_list) + ';'
    sql = 'DELETE FROM ' + table_name + ' WHERE ' + cond_list_joined
    cursor.execute(sql)
    connection.commit() # IMPORTANT
    connection.close()

def load(config, table_name, query=None):
    assert(type(config) == dict)
    assert(type(table_name) == str)
    connection = pms.connect(host = config['host'],
                        user = config['user'],
                        password = config['password'],
                        db = config['db'],
                        cursorclass=pms.cursors.DictCursor)
    cursor=connection.cursor()

    sql = 'SELECT * FROM ' + table_name
    if query != None:
        assert(type(query) == str)
        sql = query
    cursor.execute(sql)
    data = cursor.fetchall()
    connection.close()
    return data

def load_as_df(config, table_name, query=None):
    assert(type(config) == dict)
    assert(type(table_name) == str)
    if(query==None):
        data = load(config, table_name)
    else:
        data = load(config, table_name, query)
    dfs = []            
    for row in data:        # Format data into dataframe
        for item in row:
            row[item] = [row[item]]
        df = pd.DataFrame.from_dict(row)
        dfs += [df]
    if len(dfs) > 0:
        df = pd.concat(dfs, axis=0)
    else:
        raise Exception('No data was found in ' + table_name + '!')
    return df

def log(func, *args, config=None, log_table=None, desc=None, account_number=None, batch_id=None, time_limit=None):
    assert(type(config) == dict)
    assert(type(log_table) == str)
    assert(type(desc) == str)
    assert(type(account_number) == str)
    assert(type(batch_id) == str)
    log_dict = {
        'start_time':[str(datetime.datetime.now())],
        'end_time':[''],
        'status':['Start'],
        'description':[desc],
        'error_message':[''],
        'account_number':[account_number],
        'batchID':[batch_id],
        'insert_at':[str(datetime.datetime.now())]
        }
    log_df = pd.DataFrame(log_dict)
    insert(config, log_table, log_df, debug=True)
    ret=0
    try:
        if time_limit is not None:              # If a time limit is set, terminate the function if it runs longer than time_limit (seconds)
            assert(type(time_limit) == int)
            def handler(signum, frame):
                raise TimeoutError(f'Time limit of {time_limit} seconds exceeded!')

            def takes_a_long_time():
                while True:
                    print('function ongoing...')

            # Set the signal handler and an n-second alarm
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(time_limit)

        ret = func(*args)

        if time_limit is not None:
            signal.alarm(0)

    except Exception as e:
        # Delete old log entry
        cond_list = [
            'insert_at = \'' + log_dict['insert_at'][0] + '\'',
            'batchID = \'' + log_dict['batchID'][0] + '\'',
            'status = \'' + log_dict['status'][0] + '\''
        ]
        delete(config, cond_list, log_table)

        print('Failure: Inserting to ' + log_table)

        log_dict['end_time'] = [str(datetime.datetime.now())]   # Set operation end time
        log_dict['status'] = ['Failure']
        log_dict['error_message'] = [str(e)]
        log_dict['insert_at'] = [str(datetime.datetime.now())]
        log_df = pd.DataFrame(log_dict)
        # Insert new entry
        insert(config, log_table, log_df, debug=True)
        return ret
    else:
        # Delete old log entry
        cond_list = [
            'insert_at = \'' + log_dict['insert_at'][0] + '\'',
            'batchID = \'' + log_dict['batchID'][0] + '\'',
            'status = \'' + log_dict['status'][0] + '\''
        ]
        delete(config, cond_list, log_table)

        print('Success: Inserting to ' + log_table)

        log_dict['end_time'] = [str(datetime.datetime.now())]   # Set operation end time
        log_dict['status'] = ['Success']
        log_dict['error_message'] = ['']
        log_dict['insert_at'] = [str(datetime.datetime.now())]
        log_df = pd.DataFrame(log_dict)
        # Insert new entry
        insert(config, log_table, log_df, debug=True)
        return ret
    return ret

def get_access_token(config, table_name, refresh=False):
    assert(type(config) == dict)
    assert(type(table_name) == str)
    connection = pms.connect(host = config['host'],
                        user = config['user'],
                        password = config['password'],
                        db = config['db'],
                        cursorclass=pms.cursors.DictCursor)
    cursor=connection.cursor()

    sql = 'SELECT * FROM ' + table_name
    cursor.execute(sql)
    data = cursor.fetchall()

    access_token = ''

    if refresh:     # Return refresh_token instead of access token
        for row in data:
            if row['param_name'] == 'refresh_token':
                refresh_token = row['value']
                return refresh_token        

    for row in data:
        if row['param_name'] == 'access_token':
            access_token = row['value']
            return access_token
    if access_token == '':
        raise Exception('A valid access token was not found in the database')
    connection.close()

def load_etl(config, limit=6):
    
    assert(type(limit)==int)

    connection = pms.connect(host = config['host'],
                        user = config['user'],
                        password = config['password'],
                        db = config['db'],
                        cursorclass=pms.cursors.DictCursor)
    cursor=connection.cursor()

    table_name = 't_fb_load_etl'

    if limit == 6:
        sql = 'SELECT * FROM ' + table_name + ' ORDER BY start_time DESC LIMIT 6'
        cursor.execute(sql)
        data = cursor.fetchall()
        return data
    elif limit > 0:
        sql = 'SELECT * FROM ' + table_name + ' ORDER BY start_time DESC LIMIT ' + str(limit)
        cursor.execute(sql)
        data = cursor.fetchall()
        connection.close()
        return data
    else:
        connection.close()
        raise Exception('Limit must be greater than 0!')

def log_sp(config, func):
    table = 't_fb_sp_log'   # Hard coded for now
    timestamp = str(datetime.datetime.now())
    log_table = {
        'timestamp':[timestamp],
        'status':['Start']
    }
    df = pd.DataFrame(log_table)
    insert(config, table, df)
    try:
        ret = func(config)
    except Exception as e:

        delete(config, ['timestamp = \'' + timestamp + '\'', 'status = \'Start\''], table) # Delete Start entry

        timestamp = str(datetime.datetime.now())
        log_table = {
            'timestamp':[timestamp],
            'status':[str(e)]
        }
        df = pd.DataFrame(log_table)
        insert(config, table, df)
    else:

        delete(config, ['timestamp = \'' + timestamp + '\'', 'status = \'Start\''], table) # Delete Start entry

        timestamp = str(datetime.datetime.now())
        log_table = {
            'timestamp':[timestamp],
            'status':['Success']
        }
        df = pd.DataFrame(log_table)
        insert(config, table, df)
    return 0

def log_gen(config, table, desc, batch_id, func, *args):
    assert(type(config)==dict)
    assert(type(table)==str)
    assert(type(desc)==str)
    assert(type(batch_id)==str)

    tb = "No traceback recorded"


    log_dict = {
        'start_time':[str(datetime.datetime.now())],
        'end_time':[''],
        'status':['Start'],
        'description':[desc],
        'error_message':[''],
        'batchID':[batch_id],
        'insert_at':[str(datetime.datetime.now())]
    }
    log_df = pd.DataFrame(log_dict)
    insert(config, table, log_df, debug=True)

    try:
        ret = func(*args)
    except Exception as e:
        # Delete old log entry
        cond_list = [
            'insert_at = \'' + log_dict['insert_at'][0] + '\'',
            'description = \'' + log_dict['description'][0] + '\'',
            'status = \'' + log_dict['status'][0] + '\'',
            'batchID = \'' + log_dict['batchID'][0] + '\''
        ]
        delete(config, cond_list, table)
        print('Failure: Inserting to ' + table)
        tb = traceback.format_exc() # Get traceback
        log_dict['end_time'] = [str(datetime.datetime.now())]   # Set operation end time
        log_dict['status'] = ['Failure']
        log_dict['error_message'] = [str(e) + '| Traceback: ' + tb]
        log_dict['insert_at'] = [str(datetime.datetime.now())]
        log_df = pd.DataFrame(log_dict)
        # Insert new entry
        insert(config, table, log_df, debug=True)
        print(e)
        print(tb)
        return 0
    else:
        # Delete old log entry
        cond_list = [
            'insert_at = \'' + log_dict['insert_at'][0] + '\'',
            'description = \'' + log_dict['description'][0] + '\'',
            'status = \'' + log_dict['status'][0] + '\'',
            'batchID = \'' + log_dict['batchID'][0] + '\''
        ]
        delete(config, cond_list, table)
        print('Success: Inserting to ' + table)

        log_dict['end_time'] = [str(datetime.datetime.now())]   # Set operation end time
        log_dict['status'] = ['Success']
        log_dict['insert_at'] = [str(datetime.datetime.now())]
        log_df = pd.DataFrame(log_dict)
        # Insert new entry
        insert(config, table, log_df, debug=True)
        return ret

def truncate_table(config, table_name):
    connection = pms.connect(host = config['host'],
                        user = config['user'],
                        password = config['password'],
                        db = config['db'])
    cursor=connection.cursor()
    sql = 'TRUNCATE TABLE ' + table_name + ';'
    cursor.execute(sql)
    connection.commit() # Commit to save changes
    connection.close()

def delete_duplicates(config, table_name, keys):    # Only works with insert_at
    assert(type(config)==dict)
    assert(type(table_name)==str)
    assert(type(keys)==list or type(keys)==dict)
    connection = pms.connect(host = config['host'],
                        user = config['user'],
                        password = config['password'],
                        db = config['db'])
    cursor=connection.cursor()
    keys_strs = []
    for key in keys:
        keys_strs.append('t1.' + key + ' = t2.' + key)
    keys_joined = ' AND '.join(keys_strs) + ' AND t1.insert_at < t2.insert_at;'
    sql = 'DELETE t1 FROM ' + table_name + ' t1 INNER JOIN ' + table_name + ' t2 ' + \
        'WHERE ' + keys_joined
    cursor.execute(sql)
    connection.commit() # Commit to save changes
    connection.close()