from itertools import chain
from functools import lru_cache

import pandas as pd

from .db import _check_table
from .query_base import query, query_async, _check_freq
from .util import aslist


__all__ = ['distinct_values', 'all_nodes', 'all_tables', 'nodes_per_table',
           'tables_for_node', 'table_timerange']


@lru_cache()
def distinct_values(table, field,
                    nodeid='', where='', freq='10ms',
                    start_time=None, end_time=None):
    table = _check_table(table)
    where = aslist(where, str)
    freq = _check_freq(freq)
    if nodeid:
        where.append('NodeId = {!r}'.format(str(int(nodeid))))
    if start_time:
        start_time = pd.Timestamp(start_time).isoformat()
        where.append('time >= {!r}'.format(start_time))
    if end_time:
        end_time = pd.Timestamp(end_time).isoformat()
        where.append('time <= {!r}'.format(end_time))
    results = query('SELECT DISTINCT({field}) FROM {table}_{freq}{where}'.format(
        field=field,
        table=table,
        freq=freq,
        where=' AND '.join(where)))
    results = {row['distinct'] for row in results.get_points()}
    return results


@lru_cache(1)
def all_nodes():
    nodes = sorted(set(chain.from_iterable(nodes_per_table().values())), key=int)
    return nodes


@lru_cache(1)
def all_tables():
    return sorted(nodes_per_table().keys())


@lru_cache(1)
def nodes_per_table():
    results = query('SHOW TAG VALUES WITH KEY = NodeId')
    nodes = {measurement.split('_')[0]: sorted(set(row['value'] for row in rows), key=int)
             for (measurement, _), rows in results.items()}
    return nodes


@lru_cache()
def tables_for_node(nodeid):
    nodeid = str(int(nodeid))
    return {measurement
            for measurement, nodes in nodes_per_table().items()
            if nodeid in nodes}


@lru_cache()
def table_timerange(table, freq='10ms', nodeid=''):
    freq = _check_freq(freq)
    table = _check_table(table)
    where = ' WHERE NodeId = {!r}'.format(str(nodeid)) if nodeid else ''
    queries = [
        'SELECT {field} FROM {table}_{freq}{where} ORDER BY time LIMIT 1'.format(
            field=table._default_field,
            table=table,
            freq=freq,
            where=where),
        'SELECT {field} FROM {table}_{freq}{where} ORDER BY time DESC LIMIT 1'.format(
            field=table._default_field,
            table=table,
            freq=freq,
            where=where),
    ]
    times = pd.to_datetime([row['time']
                            for results in query_async(queries)
                            for _, rows in results.items()
                            for row in rows], unit='ms', utc=True)
    assert not pd.isnull(times.min())
    assert not pd.isnull(times.max())
    return times.min(), times.max()
