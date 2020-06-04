import psycopg2

from runner.logger_conf import logger


class ConnManager:
    """ConnManger is responsible for creating and maintaining the database connection object.
    Attributes:
        self.refresh_connection()
        self.exe_statement()
        self.commit_statement()
        self.get_result()
    """

    def __init__(self, cn):
        """Invoking ConnManager, creates default variables for class operation

        :param cn: Dictionary for database connection
        :type cn: dict

        - Required acceptable parameters in the dict
            database: Name of the database
            host: url/host of the database
            port: port number of the database
            user: username for the datbase (make sure the user has the priviledge to select/alter in the database)
            passw: password of the user.
        """
        self.database = cn['database']
        self.host = cn['host']
        self.port = cn['port']
        self.user = cn['user']
        self.passw = cn['passw']
        self.conn = None
        self.cur = None
        self.logger = logger

    def _set_conn(self):
        """Creates the connection with the database object.
        Logs if the there is a connection error. """
        try:
            self.conn = psycopg2.connect(dbname=self.database, user=self.user, password=self.passw, host=self.host,
                                         port=self.port)
            self.logger.info('Connection Created')
        except psycopg2.Error:
            self.logger.exception('Error establishing connection')
            return None

    def _set_cur(self):
        """Creates a cursor for the database object."""
        try:
            self.cur = self.conn.cursor()
            self.logger.info('Cursor Created')
        except psycopg2.Error:
            self.logger.exception('Error getting cursor')
            return None

    def exe_statement(self, query, commit_statement=True):
        """Execute the statement within the database object.
            Advisible for DDL, DML type statements

        :param query: The sql statement that needs to be executed.
        :param commit_statement: Commits the statement if true
        :type query: str
        :type commit_statement:bool
        :returns: 0,1,2
        :rtype: int
        - return definition:
            0: if successful
            1: if operationError
            2: if DataError
            doesn't return if uncaught exception
        """
        try:
            self.cur.execute(query)
            self.logger.info('Executed')
            if commit_statement:
                self.commit_statement()
            return 0
        except psycopg2.OperationalError:
            self.logger.exception('Operational Error')
            return 1
        except psycopg2.DataError:
            self.logger.exception('Data Error')
            return 2
        except Exception:
            self.logger.exception('Uncaught Exception')

    def get_result(self, query):
        """Executes a statement in database and return the result

        - note:
            Reserved for DQL queries only

        :param query:
        :type query: str
        :return: 2d array of executed statement result from database
        """
        try:
            self.cur.execute(query)
            return self.cur.fetchall()
        except Exception:
            self.logger.exception('Error while fetching the results')

    def commit_statement(self):
        """Commits the transaction that the current cursor has done in the connection to the database"""
        try:
            self.conn.commit()
            self.logger.info('Committed')
        except Exception:
            self.logger.exception('Error Committing')

    def _close_conn(self):
        """Closes the connection"""
        try:
            self.conn.close()
            self.logger.info('Connection Closed')
        except Exception:
            self.logger.exception('Error Closing connection')

    def refresh_connection(self):
        """Method to re-invoke a database connection."""
        if self.conn:
            self._close_conn()
        self._set_conn()
        self._set_cur()
