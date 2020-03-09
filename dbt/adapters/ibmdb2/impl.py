from dbt.adapters.sql import SQLAdapter
from dbt.adapters.ibmdb2 import IBMDB2ConnectionManager


class IBMDB2Adapter(SQLAdapter):
    ConnectionManager = IBMDB2ConnectionManager

    @classmethod
    def date_function(cls):
        return 'current_timestamp()'
    
    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode("utf-8")) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)