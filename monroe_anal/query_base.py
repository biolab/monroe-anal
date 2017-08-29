import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

from .connection import get_client, InfluxDBException, ResultSet
from .util import aslist
from .db import _check_table, _CATEGORICAL_COLUMNS


__all__ = ['query', 'query_async', 'getdf']


log = logging.getLogger(__name__)


def query(query, **kwargs) -> ResultSet:
    try:
        client = get_client()
    except InfluxDBException:
        log.exception('Failed to instantiate InfluxDB client:')
        raise
    try:
        log.debug('Executing query: %s', query)
        result = client.query(query, **kwargs)
        log.debug('Result set size: %d, %d rows', len(result), len(tuple(result.get_points())))
        return result
    except InfluxDBException:
        log.exception('Failed to process query: %s', query)
        raise


def query_async(queries, **kwargs) -> ResultSet:
    with ThreadPoolExecutor(max_workers=len(queries)) as executor:
        for future in as_completed(executor.submit(query, query_str, epoch='ms', **kwargs)
                                   for query_str in queries):
            yield future.result()


def _query_str(table, *, where='', resample='', limit=1000):
    parts = ['SELECT {columns} FROM {table}'.format(table=str(table),
                                                    columns=table._select())]
    if where:
        where = aslist(where, str)
        parts.append('WHERE ' + ' AND '.join(where))

    if resample:
        resample = 'time({}), '.format(resample)

    parts.append('GROUP BY ' + (resample + table._groupby()).lstrip(','))

    if limit:
        parts.append('LIMIT ' + str(int(limit)))

    query_str = ' '.join(parts)
    return query_str


def _where_field_name(condition, _split=re.compile(r'\W').split):
    return _split(condition, maxsplit=1)[0]


def getdf(tables, *, nodeid='', where='', limit=1000,
          start_time=None, end_time=None,
          resample='30s',
          interpolate=False) -> pd.DataFrame:

    tables = list(map(_check_table, aslist(tables)))
    if not tables:
        raise ValueError('Need a table name to fetch')

    if where and isinstance(where, str):
        where = [where]
    where = aslist(where or [], str)

    if nodeid:
        nodeid = aslist(nodeid, str)
        nodedid_where = ['NodeId = {!r}'.format(str(node))
                         for node in nodeid]
        where.extend('(' + ' OR '.join(nodedid_where) + ')')

    from .queries import table_timerange
    if end_time is None:
        max_time = max(i[1] for i in map(table_timerange, tables) if i)
        end_time = min(pd.Timestamp('now', tz='UTC'), max_time)
    if start_time is None:
        min_time = min(i[0] for i in map(table_timerange, tables) if i)
        start_time = max(pd.Timestamp.fromordinal(end_time.toordinal() - 14, tz='UTC'),
                         min_time)
    # Sanitize input date and time
    start_time = pd.Timestamp(start_time, tz='UTC').isoformat()
    end_time = pd.Timestamp(end_time, tz='UTC').isoformat()

    where.append('time >= {!r}'.format(start_time))
    where.append('time <= {!r}'.format(end_time))

    queries = []
    for table in tables:
        table_columns = {'time'} | set(table._columns())
        _where = [cond for cond in where
                  if _where_field_name(cond) in table_columns]
        queries.append(_query_str(table, where=_where,
                                  resample=resample, limit=limit))

    dfs = []
    for results in query_async(queries):
        for (measurement, tags), rows in results.items():
            df = pd.DataFrame(list(rows))
            df.fillna(np.nan, inplace=True)
            df.index = pd.to_datetime(df.time, unit='ms')
            df.drop(['time'], axis=1, inplace=True)
            for tag, value in tags.items():
                df[tag] = value
            df = _check_table(measurement).__transform__(df)
            df.columns = ['{}_{}'.format(measurement, column)
                          for column in df.columns]
            dfs.append(df)

    df = pd.concat(dfs)
    del dfs
    df.sort_index(inplace=True)

    # Transform known categorical columns into Categoricals
    for col in df:
        if col.endswith(_CATEGORICAL_COLUMNS):
            df[col] = df[col].astype('category')

    if interpolate:
        if interpolate is True:
            interpolate = 'linear'
        if interpolate == 'ffill':
            df.ffill(inplace=True)
        elif interpolate == 'bfill':
            df.bfill(inplace=True)
        else:
            df.interpolate(method=interpolate, inplace=True)

    df.dropna(inplace=True)
    return df

