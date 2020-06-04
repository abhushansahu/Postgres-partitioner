import os
import time

from runner.conf import database_connection_string, table_list, cfg_complete_run, cfg_are_you_sure, cfg_verified, cfg_divisor
from runner.conn_manager import ConnManager
from runner.logger_conf import logger

__author__ = "Abhushan Sahu"


def read_file(c_path):
    """
    Returns the File data
    :param c_path: Str of file location
    :return: Str of read data
    """
    path = os.path.abspath(os.path.join(os.pardir, c_path))
    with open(path, 'r') as f:
        query = f.read()
    return query


def get_all_cols(with_type=False):
    """
    Returns a string that contians all the columns in the table, provided in the conf (all_columns).
    :param with_type: Bool, default False, to define the if the result needs to be with datatype
    :return: Str of all columns
    """
    all_cols = ''
    if with_type:
        for k, v in table_data['all_columns'].items():
            all_cols += k + ' ' + v + ', '
    else:
        for k, v in table_data['all_columns'].items():
            all_cols += k + ', '
    return all_cols


def timeit(method):
    """
    Way to measure the execution of a method body.
    :param method: Function to run
    :return: timed() function.
    """

    def timed(*args, **kw):
        ts = time.perf_counter()
        result = method(*args, **kw)
        te = time.perf_counter()
        logger.info('{0}, {1} s'.format(method, str(te - ts)))
        return result

    return timed


@timeit
def clear_temp():
    """
    Drops and recreates the intermediary temporary table, to fight off boating.
    :return: None
    """
    query = read_file('assets/scripts/ddl/temp_table.sql')
    query = query.format(table_data['temp_table_name'], table_data['table_name'], table_data['new_date_column_name'])
    logger.info(query[:50])
    cm.exe_statement(query)
    logger.info('Temporary table restored')


@timeit
def inconsistent_data(_1, _2):
    """
    Moves the inconsistent data in the range to inconsistent table.
    :param _1: Integer that defines start limit
    :param _2: Integer that defines the end limit.
    :return: None
    """
    query = read_file('assets/scripts/dml/inconsistent_insert.sql')
    query = query.format(_1 * divisor, table_data['short_hand_name_for_child'], table_data['table_name'],
                         table_data['date_column_name'], table_data['primary_column'], _2 * divisor)
    # print(query)
    looper_exe(query)
    logger.info('Executed for inconsistent')


def looper_exe(i, _1=0, _2=0, commit_statement=True):
    """
    Executes the SQL queries provided.

    :param i: Query to be executed.
    :param _1: Defines the start limit of execution
    :param _2: Defines the end limit of execution
    :param commit_statement: If the query needs to be committed. Default is true.
    :return: None
    """
    logger.info(i[:50])
    res = cm.exe_statement(i, commit_statement)
    if res == 0:
        logger.info('Executed Successfully')
    elif res == 1:
        cm.refresh_connection()
        logger.info('Recreated connection during statement operational error')
        cm.cur.exe_statement(i, commit_statement)
    elif res == 2:
        # For Data Error, which is mostly amounted by inconsistent data.
        cm.refresh_connection()
        inconsistent_data(_1, _2)
        # Writing custom query to append only non-inconsistent data to the temp table.
        if i[:17] == 'insert into temp_':
            i = i[:-2] + ' and coalesce({0}) ~ \'{1}\';'.format(
                table_data['date_column_name'], r'^[\d]{4}-[0-1][1-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]$')
            logger.info(i)
            cm.exe_statement(i, commit_statement)
        else:
            logger.warning('Unknown Exception Occurred, please check urgently the flow of the code')


def loopee(query, _1, _2):
    """
    Loops through each iteration from start to end, thus providing limit(equal to divisor) for execution
    :param query: String query to be exectuted on the database
    :param _1: start limit
    :param _2: end limit
    :return: None
    """
    s = time.perf_counter()
    query = query.format(_1 * divisor, _2 * divisor)
    # Splits the query form the proposed file, as it contains some non-loopable code.
    query_breakdown = query.split('|')
    for i in query_breakdown:
        looper_exe(i, _1, _2)
    logger.info('Ending -------------- Took {0} s'.format(str(time.perf_counter() - s)))


@timeit
def bulk_insert(start, end):
    """
    Invokes the method which inserts from current master table to the temporary table. From the proposed script file.
    Pauses and recreates the connection after every 5 attempts.

    :param start: Integer to define the stard of loop
    :param end: Integer to define the end of loop
    :return: None
    """
    query = read_file('assets/scripts/dml/bulk_insert.sql')
    query = query.format('{0}', table_data['temp_table_name'], table_data['table_name'],
                         table_data['short_hand_name_for_child'], table_data['new_date_column_name'],
                         table_data['date_column_name'], table_data['primary_column'], get_all_cols(), '{1}')
    for _ in range(start, end):
        logger.info('Starting ------------- {0} of {1}'.format(str(_), str(end)))
        loopee(query, _, _ + 1)
        if _ % 5 == 0:
            clear_temp()
            cm.refresh_connection()


@timeit
def create_child_table():
    """
    Creates the new child table(s) from the proposed script file.
    :return: None
    """
    from datetime import datetime, timedelta
    start = datetime.strptime("2019-01-01", '%Y-%m-%d')
    end = datetime.now()
    m_list = set([(start + timedelta(_)).strftime(r"%Y_%m") for _ in range((end - start).days)])
    query = read_file('assets/scripts/ddl/create_child_table.sql')
    query = query.format(table_data['new_table_name'], table_data['short_hand_name_for_child'],
                         table_data['primary_column'], table_data['new_date_column_name'], '{0}')
    query = query.split('|')
    final_query = query[0]
    for _ in m_list:
        final_query += query[1].format(_)
    looper_exe(final_query)
    logger.info('Child Table Created with prefix {0}'.format(table_data['short_hand_name_for_child']))


@timeit
def create_trigger():
    """
    Creates the trigger over the new table, provided by the proposed script file
    :return: None
    """
    query = read_file('assets/scripts/ddl/create_trigger.sql')
    query = query.format(table_data['new_table_name'])
    looper_exe(query)
    logger.info('Trigger created on {0}'.format(table_data['new_table_name']))


@timeit
def analyze_master():
    """
    Runs the Analyze command to run on master, provided from the proposed script file.
    :return: None
    """
    query = read_file('assets/scripts/health/analyze_master.sql')
    query = query.format(table_data['table_name'])
    looper_exe(query)
    logger.info('Table Analysis Completed of {0}'.format(table_data['table_name']))


@timeit
def delete_master():
    """
    Drops the old master table from existance.
    :return: None
    """
    query = read_file('assets/scripts/dml/bulk_delete.sql')
    query = query.format(table_data['backup_table_name'])
    looper_exe(query)
    logger.info('Table deleted {0}'.format(table_data['backup_table_name']))


@timeit
def create_function():
    query = read_file('assets/scripts/ddl/create_function.sql')
    query = query.format(table_data['new_table_name'],
                         table_data['new_date_column_name'],
                         table_data['short_hand_name_for_child'],
                         table_data['primary_column'] + ', ' + ', '.join(table_data['all_columns']),
                         'NEW.' + table_data['primary_column'] + ', NEW.' + ', NEW.'.join(table_data['all_columns']),
                         ', '.join(['$' + str(_) for _ in range(1, len(table_data['all_columns']) + 2)]),
                         table_data['table_name'],
                         ', '.join(['$' + str(_) for _ in range(1, len(table_data['all_columns']) + 3)]))

    # print(query)
    looper_exe(query)
    logger.info('Function for Table Created')


@timeit
def create_master_table():
    """
    Creates the new master table from the proposed script file.
    :return: None
    """
    query = read_file('assets/scripts/ddl/create_master_table.sql')
    query = query.format(table_data['new_table_name'], table_data['primary_column'],
                         table_data['new_date_column_name'], get_all_cols(with_type=True))
    looper_exe(query)
    logger.info('Master Table Created {0}'.format(table_data['new_table_name']))


def find_min_max(batched=False, rounded=True):
    """
    Return the min/max of the master table(s)

    - find_min_max(batched=True, rounded=True): Ceil of Divisor divided max from new_master, Ceil of Divisor divided max
     from old_master
    - find_min_max(batched=True, rounded=False): Max from new_master, Max from old_master
    - find_min_max(batched=False, rounded=True): Floor of Divisor divided min from old_master, Ceil of Divisor divided
     max from old_master
    - find_min_max(batched=False, rounded=True): min from old_master, max from old_master


    :param batched: Booelan value to define the master for the value extraction
    :param rounded: Boolean value to define if return should be in int or float
    :return: int,int/float,float
    """
    q = 'select min({0}), max({0}) from {1}'
    query = q.format(table_data['primary_column'], table_data['table_name'])
    res = cm.get_result(query)
    import math
    if batched:
        query = q.format(table_data['primary_column'], table_data['new_table_name'])
        res2 = cm.get_result(query)
        if rounded:
            return math.ceil(res2[0][1] / divisor), math.ceil(res[0][1] / divisor)
        else:
            return res2[0][1], res[0][1]
    if rounded:
        return math.floor(res[0][0] / divisor), math.ceil(res[0][1] / divisor)
    else:
        return res[0][0], res[0][1]


def create_all():
    """
    Orchestration for table creation
    :return:
    """
    # analyze_master() Disabling as it is taking a lot of time to run.
    create_master_table()
    create_child_table()
    clear_temp()


def switch_masters(are_you_sure=False, verified=False):
    """

    :param are_you_sure: Boolean parameter to confirm if trur then code needs to switch the master table with the newer
     table
    :param verified: Boolean parameter to confirm if true then delete the master table
    :return:
    """
    # Refresh connection
    cm.refresh_connection()
    # Create the function to be caller for newer inserts
    create_function()
    # Create the trigger to be called for the new table/ for new inserts
    create_trigger()
    # Locks the current master table to stop the data input.
    lock_table = 'Lock table {0} in EXCLUSIVE mode;'.format(table_data['table_name'])
    looper_exe(lock_table, False)
    # Find the max value from the new master table
    max_val = 'select max(id) from {0};'.format(table_data['new_table_name'])
    # Change the squence id to make sure newer inserts works after the last insert primary  key
    """
    Proposed argument by __author__ to produce the max sequence from current master rather than new master
    """
    sequence = 'ALTER SEQUENCE {0} RESTART WITH {1};' \
        .format(table_data['table_sequence_id'], str(cm.get_result(max_val)[0][0] + 1))
    looper_exe(sequence, False)
    # Alter name statement for current and new masters
    alter = 'alter table {0} rename to {1};'
    alter_1 = alter.format(table_data['table_name'], table_data['backup_table_name'])
    alter_2 = alter.format(table_data['new_table_name'], table_data['table_name'])
    if are_you_sure:
        looper_exe(alter_1, False)
        logger.info('Master moved to backup')
        looper_exe(alter_2, False)
        logger.info('New became master')
        if verified:
            # To drop the master (old) tables
            delete_master()
    else:
        print('Run these statements')
        print(alter_1)
        print(alter_2)
    # Committing the transaction.
    cm.commit_statement()


@timeit
def main(complete_run=False):
    """
    Responsible for orchestrating the run by calling the functions
        create_all()
        bulk_insert()

    :param complete_run: Argument to define whether or not to run the full run.
    ":type complete_run: bool
    :return: None
    """
    cm.refresh_connection()
    if complete_run:
        create_all()
        s, e = find_min_max()
        bulk_insert(s, e)
    else:
        s, e = find_min_max(batched=True)
        clear_temp()
        bulk_insert(s, e)


if __name__ == "__main__":
    for _ in table_list:
        table_data = _
        divisor = cfg_divisor
        cm = ConnManager(database_connection_string)
        main(complete_run=cfg_complete_run)
        switch_masters(are_you_sure=cfg_are_you_sure, verified=cfg_verified)
