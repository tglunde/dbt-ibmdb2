from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager


DBT-IBMDB2_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
    },
    'required': ['database', 'schema'],
}


class IBMDB2Credentials(Credentials):
    SCHEMA = DBT-IBMDB2_CREDENTIALS_CONTRACT

    @property
    def type(self):
        return 'dbt-ibmdb2'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        raise NotImplementedError


class IBMDB2ConnectionManager(SQLConnectionManager):
    TYPE = 'dbt-ibmdb2'
