from enum import IntEnum as _IntEnum

import numpy as np


# TABLE_ALIAS = {
#     'ping': 'monroe_exp_ping',
#     'gps': 'monroe_meta_device_gps',
# }

TABLE_COLUMNS = {
    'ping': {'NnodeId', 'Timestamp', 'Operator', 'Iccid', 'Host', 'Rtt'},
    'gps': {'NodeId', 'Timestamp', 'Latitude', 'Longitude', 'Altitude', 'Speed', 'SatelliteCount'},
}

class _TableMeta(type(_IntEnum)):
    def __str__(cls):
        return getattr(cls, '__fullname__', cls.__name__)


class _Table(metaclass=_TableMeta):
    def __str__(self):
        return self.name


class ping(_Table):
    __fullname__ = 'monroe_exp_ping'
    NodeId, Timestamp, Operator, Iccid, Host, Rtt = 2**np.arange(6)

class gps(_Table):
    __fullname__ = 'monroe_meta_device_gps'
    NodeId, Timestamp, Latitude, Laongitude, Altitude, Speed, SatelliteCount = 2**np.arange(7)


def _known_tables():
    return [T for name, T in globals().items()
            if isinstance(T, _Table.__class__) and not name.startswith('_')]


def _check_table(table):
    return _table_obj(table)


def _table_obj(table_name):
    try:
        return next(T for T in _known_tables()
                    if T is table_name or T.__name__ == table_name)
    except StopIteration:
        raise ValueError('Unknown MONROE table: {}'.format(table_name))


def _check_columns(table, columns):
    table = _table_obj(table)
    valid_cols = {'*'} | set(map(str.lower, table.__members__))
    for col in columns:
        if not col in valid_cols:
            raise ValueError("Unknown column '{}' for table '{}'".format(col, table))
    return columns

