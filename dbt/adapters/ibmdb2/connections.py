from dataclasses import dataclass
from contextlib import contextmanager

import time
import dbt
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger

import jaydebeapi
import jpype


@dataclass
class IBMDB2Credentials(Credentials):
    host: str
    port: int
    username: str
    password: str
    database: str
    schema: str

    @property
    def type(self):
        return 'ibmdb2'

    def _connection_keys(self):
        return ('host', 'port', 'username', 'database', 'schema')


class IBMDB2ConnectionManager(SQLConnectionManager):
    TYPE = 'ibmdb2'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection
        credentials = cls.get_credentials(connection.credentials)
        try:
            if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
                jpype.attachThreadToJVM()
                jpype.java.lang.Thread.currentThread().setContextClassLoader(jpype.java.lang.ClassLoader.getSystemClassLoader())
            
            C = jaydebeapi.connect('com.ibm.db2.jcc.DB2Driver',
                                    'jdbc:db2://' + credentials.host + ':' + str(credentials.port) + '/' + credentials.database,
                                    [credentials.username, credentials.password],
                                    'C:/Users/ilija/Downloads/db2jcc-db2jcc4.jar')
            connection.handle = C
            connection.state = 'open'

        except Exception as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        connection_name = connection.name
        connection.abort_query()

    @classmethod
    def get_status(cls, cursor):
        return 'OK'

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass