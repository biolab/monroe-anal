import re
import logging
from datetime import datetime
import itertools

import pandas as pd
from cassandra import DriverException
from cassandra.concurrent import execute_concurrent

from .util import aslist, lower
from .connection import get_session
from .db import (_check_table, _check_columns,
                 _CATEGORICAL_COLUMNS, _CAST_TYPES, _CAST_RENAMES)


__all__ = ['query', 'getrows', 'getdf']


log = logging.getLogger(__name__)


def query(query, *args, **kwargs) -> tuple:
    params = args[0] if len(args) else kwargs.get('parameters', '')
    with get_session() as session:
        log.debug('Executing query with parameters: %s -- (%s)', query, params)
        try:
            return tuple(session.execute_async(query, *args, **kwargs).result())
        except DriverException:
            log.exception('Failed to process query: %s', query)
            raise


def _query_str(table, columns, *, distinct=False, where='', groupby='', orderby='', limit=1000,
               partition_limit=None, _allow_filtering=False):
    parts = [
        'SELECT {distinct}{columns} FROM {table}'.format(
            distinct='DISTINCT ' if distinct else '',
            table=str(table),
            columns=', '.join(columns))]
    if where:
        where = aslist(where)
        parts.append('WHERE ' + ' AND '.join(where))
        _allow_filtering = True

    if groupby:
        parts.append('GROUP BY ' + groupby)

    if orderby:
        parts.append('ORDER BY ' + orderby)

    if partition_limit:
        parts.append('PER PARTITION LIMIT ' + partition_limit)

    if limit:
        parts.append('LIMIT ' + str(int(limit)))

    if _allow_filtering:
        parts.append('ALLOW FILTERING')

    query_str = ' '.join(parts)
    return query_str


def select(table, columns='*', *, where='', groupby='', orderby='', limit=1000,
           _allow_filtering=False) -> tuple:
    table = _check_table(table)
    columns = _check_columns(table, aslist(columns))
    query_str = _query_str(table, columns, where=where, groupby=groupby,
                           orderby=orderby, limit=limit)
    return query(query_str)


def getdf(table_spec, *, where='', groupby='', orderby='', limit=1000,
          start_time=None, end_time=None,
          resample='30s',
          interpolate=False) -> pd.DataFrame:

    if where and isinstance(where, str):
        where = [where]
    where = aslist(where or [], str)
    EMPTY_PARAM = object()  # To have `params` length match that of `where`. These params are skipped.
    params = [EMPTY_PARAM] * len(where)

    if end_time is None:
        end_time = datetime.now()
    if start_time is None:
        start_time = datetime.fromordinal(end_time.toordinal() - 14)
    # Sanitize input date and time; Monroe DB schema expects it as Decimal
    start_time = int(pd.Timestamp(start_time).timestamp())
    end_time = int(pd.Timestamp(end_time).timestamp())

    where.append('Timestamp >= %s')
    params.append(start_time)
    where.append('Timestamp <= %s')
    params.append(end_time)

    table_spec = aslist(table_spec, str)
    if not table_spec:
        raise ValueError('Need a table name to fetch')

    queries = []
    query_params = []
    tables = []
    for table, group in itertools.groupby(sorted(table_spec),
                                          key=lambda s: s.split('.', 1)[0]):
        table = _check_table(table)
        columns = _check_columns(table, group)
        all_table_columns = set(lower(_check_columns(table, '*')))
        tables.append(table)
        if 'timestamp' not in lower(columns) and 'timestamp' in all_table_columns:
            columns.append('Timestamp')

        # Cast columns to "better" types
        columns = _typecast_columns(columns)

        # Each where condition affects only those tables that contain the field
        where_params = [(cond, param) for cond, param in zip(where, params)
                        if _where_field_name(cond).lower() in all_table_columns]
        table_where, table_params = zip(*where_params) if where_params else ('', ())
        query_params.append([p for p in table_params if p is not EMPTY_PARAM])

        queries.append(_query_str(table, columns, where=table_where,
                                  groupby=groupby, orderby=orderby,
                                  limit=limit))

    df = []
    with get_session() as session:
        # params = [params]*len(queries)
        for query, param in zip(queries, query_params):
            log.debug('Executing query with parameters: %s -- (%s)', query, param)

        try:
            results = execute_concurrent(session, zip(queries, query_params),
                                         concurrency=20)
        except DriverException:
            log.exception('Error processing queries:')
            raise

        for table, (success, result) in zip(tables, results):
            if not success:
                log.exception('Error occurred: %s', result)
                raise result

            df1 = pd.DataFrame(list(result))
            if not df1.empty:
                df1.rename(columns=_CAST_RENAMES, inplace=True)
                df1 = table.__transform__(df1)
                df1.index = pd.to_datetime(df1.timestamp.astype(float), unit='s')
                df1.drop(['timestamp'], axis=1, inplace=True)
            df.append(df1)

    df = pd.concat(df)

    # Transform known categorical columns into Categoricals
    categorical_cols = list(set(df.columns) &
                            set(lower(_CATEGORICAL_COLUMNS)))
    for col in categorical_cols:
        df[col] = df[col].astype('category')

    if resample and not df.empty:
        # Group by categorical values and resample each group
        grouped = df.groupby(categorical_cols) if categorical_cols else df
        resampled = grouped.resample(resample)
        try:
            df = resampled.mean()
        except pd.core.base.DataError:  # No numeric columns to aggregate
            pass
        else:
            # Groupby puts group keys as index; revert this to timestamp index
            df.reset_index(df.index.names[:-1], inplace=True)

    df.sort_index(inplace=True)

    if interpolate:
        if interpolate is True:
            interpolate = 'linear'
        if interpolate == 'ffill':
            df.ffill(inplace=True)
        elif interpolate == 'bfill':
            df.bfill(inplace=True)
        else:
            df.interpolate(method=interpolate, inplace=True)

    return df


def _where_field_name(condition, _split=re.compile('\W').split):
    return _split(condition, maxsplit=1)[0]


def _typecast_columns(columns):
    columns = columns.copy()
    for i, col in enumerate(columns):
        if col in _CAST_TYPES:
            columns[i] = 'cast({} as {})'.format(col, _CAST_TYPES[col])
    return columns
