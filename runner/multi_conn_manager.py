import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from runner.logger_conf import logger

# Defines the number of parallel connection
number_of_partition = 10


class MultiConnManager:
    """
    For creating parallel connection.
    """

    def __init__(self, conn_string):
        """

        :param conn_string: connection string dictionary
        """
        self.attempt = 3
        self.conn_string = conn_string
        self.cur = None

    def create_conn(self):
        """
        Create connection
        :return: ParallelConnection.cursor
        """
        try:
            pools = []
            for _ in range(number_of_partition):
                pools.append(ThreadedConnectionPool(1, number_of_partition, host=self.conn_string['host'],
                                                    port=self.conn_string['port'],
                                                    database=self.conn_string['database'],
                                                    user=self.conn_string['user'],
                                                    password=self.conn_string['passw']))
            connections = [p.getconn() for p in pools]
            pdb = ParallelConnection(connections)
            self.cur = pdb.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logger.info("Connection created")
            return self.cur
        except Exception as e:
            logger.error('Attempt Failed: {0}'.format(e))

    def close_conn(self):
        """
        Closes the connection
        :return: None
        """
        try:
            self.cur.close()
            logger.info('Connection Closed')
        except Exception as e:
            logger.error('Error closing connection - {0}'.format(e))


class ParallelConnection(object):
    """
    Worker class for the parallel connection
    """

    def __init__(self, connections):
        """

        :param connections: Parallel connection object
        """
        self.connections = connections
        self.cursors = None

    def cursor(self, *args, **kwargs):
        """
        Creates the parallel connections
        """
        self.cursors = [connection.cursor(*args, **kwargs) for connection in self.connections]
        return self
