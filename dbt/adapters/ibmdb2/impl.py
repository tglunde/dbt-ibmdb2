from dbt.adapters.sql import SQLAdapter
from dbt.adapters.dbt-ibmdb2 import IBMDB2ConnectionManager


class IBMDB2Adapter(SQLAdapter):
    ConnectionManager = IBMDB2ConnectionManager
