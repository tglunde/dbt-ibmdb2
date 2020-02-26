from dbt.adapters.dbt-ibmdb2.connections import IBMDB2ConnectionManager
from dbt.adapters.dbt-ibmdb2.connections import IBMDB2Credentials
from dbt.adapters.dbt-ibmdb2.impl import IBMDB2Adapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import dbt-ibmdb2


Plugin = AdapterPlugin(
    adapter=IBMDB2Adapter,
    credentials=IBMDB2Credentials,
    include_path=dbt-ibmdb2.PACKAGE_PATH)
