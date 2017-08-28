import pandas as pd
from datetime import datetime

from .db import _all_tables, _check_table, _check_columns
from .query_base import query, getdf

def unique_values(table_name, column_name='NodeId',
                  start_time=None, end_time=None):
    # table = _check_table(table_name)
    # column = _check_columns(table, column_name)
    # table_spec = '{}.{}'.format(table, column)
    # getdf(table_spec, )
    # return getdf(table_name, )

    # raise RuntimeError
    return query('SELECT DISTINCT nodeid FROM {} WHERE timestamp > 121231223 ALLOW FILTERING'.format(table_name))


# @cache(10)
def all_nodes(start_time=None, end_time=None):
    ONE_MONTH_AGO = datetime.fromordinal(datetime.now().toordinal() - 30)
    start_time = pd.Timestamp(start_time or ONE_MONTH_AGO).timestamp()
    end_time = pd.Timestamp(end_time or datetime.now()).timestamp()
    rows = query('SELECT DISTINCT NodeId '
                 'FROM monroe_meta_node_event '
                 'WHERE Timestamp > %s AND Timestamp < %s '
                 'ALLOW FILTERING',
                 (start_time, end_time))
    nodes = [i[0] for i in rows]
    return nodes


# def minmax_time(nodeid, start_time=None, end_time=None):
#     ONE_MONTH_AGO = datetime.fromordinal(datetime.now().toordinal() - 30)
#     start_time = pd.Timestamp(start_time or ONE_MONTH_AGO).timestamp()
#     end_time = pd.Timestamp(end_time or datetime.now()).timestamp()
#     rows = query('SELECT DISTINCT NodeId '
#                  'FROM monroe_meta_node_event '
#                  'WHERE Timestamp > %s AND Timestamp < %s '
#                  'ALLOW FILTERING',
#                  (start_time, end_time))
#     nodes = [i[0] for i in rows]
#     return nodes


# unique nodes from table where time
# time range for each node for each table

# for each table list of nodes
# for each node for each table timestamp range
# for each node list of tables


def nodes_for_table(table):
    table = _check_table(table)
    table.__NODEID_KEYS

def tables_for_node(nodeid):
    nodeid = str(int(nodeid))
    for table in _all_tables():
        'SELECT COUNT(*) FROM {table} WHERE NodeId = {nodeid}'

def node_table_timerange(nodeid, table):
    table = _check_table(table)
    'SELECT COUNT(*), MIN(Timestamp), MAX(Timestamp) FROM {table} WHERE NodeId = {nodeid}'
