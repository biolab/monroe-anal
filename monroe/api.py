import logging

import pandas as pd
from cassandra import DriverException

from .util import aslist
from .connection import new_session
from .dbschema import *
from .dbschema import _check_table, _check_columns


__all__ = ['query', 'getrows', 'getdf']


log = logging.getLogger(__name__)


def query(query, *args, **kwargs) -> tuple:
    params = args[0] if len(args) else kwargs.get('parameters', '')
    log.debug('Executing query with parameters: %s -- (%s)', query, params)
    with new_session() as session:
        try:
            return tuple(session.execute_async(query, *args, **kwargs).result())
        except DriverException:
            log.exception('Failed to process query: %s', query)
            raise


def select(table, columns='*', *, where='', limit=1000, allow_filtering=False) -> tuple:
    columns = _check_columns(table, aslist(columns))
    table = _check_table(table)

    parts = ['SELECT {columns} FROM {table}'.format(table=str(table),
                                                    columns=', '.join(columns))]
    if where:
        where = aslist(where)
        parts.append('WHERE ' + ' AND '.join(where))
        log.warning("'WHERE' query requires 'ALLOW FILTERING")
        allow_filtering = True

    if limit:
        parts.append('LIMIT ' + str(int(limit)))

    if allow_filtering:
        parts.append('ALLOW FILTERING')

    query_str = ' '.join(parts)
    return query(query_str)


def getdf(tables_cols, *, where='', limit=1000,
          start_time=None, end_time=None,
          interpolate=False) -> pd.DataFrame:

    if start_time is not None:
        start_time = pd.Timestamp(start_time).to_datetime()
    if end_time is not None:
        end_time = pd.Timestamp(end_time).to_datetime()

    where = aslist(where or [])
    params = []

    if start_time:
        where.append('Timestamp >= %s')
        params.append(start_time)
    if end_time:
        where.append('Timestamp <= %s')
        params.append(end_time)

    for table, columns in tables_cols:
        rows = select(table, columns)
    ...
