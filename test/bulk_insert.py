import time

from runner.conf import database_connection_string
from runner.conn_manager import ConnManager
from runner.multi_conn_manager import MultiConnManager


def global_number():
    """
    Return the serialized number incremented by 1 every time this function is called.
    :return: int
    """
    global global_num
    global_num += 1
    return global_num


def main():
    """
    1. Creates threaded connection
    2. executes the statement spanning across multiple connection
    :return: None
    """
    co = MultiConnManager(database_connection_string)
    curs = co.create_conn()
    print(curs)
    while True:
        try:
            for _ in range(len(curs.cursors)):
                curs.cursors[_].execute(
                    'insert into {0}({1}) values ({2})'.format(
                        table_name,
                        column_name,
                        global_number()))
                curs.connections[_].commit()
                print(_)
                time.sleep(_ / 10)
        except Exception as e:
            print(e)
    # for _ in range(len(curs.cursors)):
    #     a = curs.cursors[_].fetchall()
    #     print(a)


def clean(not_for_production=False):
    """
    deletes the data from the table
    :param not_for_production: deletes if true
    ":type not_for_production: bool
    :return: None
    """
    if not_for_production:
        print('About to delete the data')
        time.sleep(10)
        co = ConnManager(database_connection_string)
        co.refresh_connection()
        co.exe_statement('delete from {0}'.format(table_name))
        co.commit_statement()
    else:
        pass


if __name__ == "__main__":
    table_name = ''
    column_name = ''
    production = False
    if table_name != '' and column_name != '' and not production:
        global_num = 0
        clean(not_for_production=production)
        main()
    else:
        print('Enter table name to test and column name to test (column which is serial)')
