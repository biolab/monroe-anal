import sys
from collections import OrderedDict


_GROUP_BY = object()


class _TableMeta(type):
    def __repr__(cls):
        return cls.__name__

    # Preserve members ordering in Py<3.6
    if sys.version_info < (3, 6):
        def __prepare__(*args, **kwargs):
            return OrderedDict()


class _Table(metaclass=_TableMeta):
    __table_name__ = None  #: Override if subclass name not equal to DB table name

    def __transform__(self, df):
        """
        Override. Apply any post-receive transformation on the DataFrame,
        such as decoding categorical ints into human-readable values.
        """
        return df

    def __repr__(self):
        return str(self.__class__)

    def __iter__(self):
        return iter((attr, getattr(self, attr))
                    for attr in dir(self)
                    if not attr.startswith('_'))

    def _select(self):
        return ', '.join('{0}({1}) as {1}'.format(aggfunc, col)
                         for col, aggfunc in self
                         if aggfunc is not _GROUP_BY)

    def _groupby(self):
        return ', '.join(col
                         for col, aggfunc in self
                         if aggfunc is _GROUP_BY)

    def _columns(self):
        return [col for col, _ in self]


_CATEGORICAL_COLUMNS = (
    'NodeId',
    'Operator',
    'Iccid',
    'Host',
    'EventType',
    'Message',
    'CID',
    'DeviceMode',
    'DeviceState',
    'Frequency',
    'IP_Addr',
    'MCC_MNC',
)


class ping(_Table):
    NodeId = Iccid = _GROUP_BY
    RTT = 'MEAN'
    Error = 'SUM'
    Operator = Host = 'MODE'
    _default_field = 'RTT'


class gps(_Table):
    NodeId = _GROUP_BY
    Latitude = Longitude = Altitude = Speed = SatelliteCount = 'MEAN'
    _default_field = 'Latitude'


class sensor(_Table):
    NodeId = _GROUP_BY
    Temperature = CPU_User = CPU_Apps = Free = Swap = bat_usb0 = bat_usb1 = bat_usb2 = 'MEAN'
    BootCounter = Uptime = CumUptime = 'MAX'
    _default_field = 'Temperature'


class event(_Table):
    NodeId = _GROUP_BY
    EventType = Message = 'MODE'
    _default_field = 'EventType'


class modem(_Table):
    NodeId = Iccid = _GROUP_BY
    CID = DeviceMode = DeviceState = Frequency = MCC_MNC = Operator = IP_Addr = 'MODE'
    ECIO = RSRQ = RSSI = 'MEAN'
    _default_field = 'DeviceMode'

    def __transform__(self, df):
        # from https://github.com/MONROE-PROJECT/data-exporter
        if 'DeviceMode' in df:
            df.DeviceMode.replace(
                dict(enumerate(('unknown', 'disconnected', 'no_service', '2G', '3G', 'LTE'), 1)),
                inplace=True)
        if 'DeviceState' in df:
            df.DeviceState.replace(
                dict(enumerate(('unknown', 'registered', 'unregistered', 'connected', 'disconnected'))),
                inplace=True)
        return df


# Hide .mro() member by exposing instances instead of types
ping = ping()
gps = gps()
sensor = sensor()
event = event()
modem = modem()
for name, table_type in list(globals().items()):
    if not name.startswith('_') and isinstance(table_type, type) and issubclass(table_type, _Table):
        globals()[name] = table_type()


def _all_tables():
    for name, T in globals().items():
        if isinstance(T, _Table) and not name.startswith('_'):
            yield T


def _check_table(table):
    if isinstance(table, _Table):
        return table
    try:
        return next(T for T in _all_tables()
                    if (T is table or
                        T.__class__.__name__ == table))
    except StopIteration:
        raise ValueError('Unknown MONROE table: {}'.format(table))




# Unit tests

assert str(ping) == 'ping'
assert str(ping.__class__) == 'ping'
assert len(list(iter(ping))) == 6
assert len(ping._columns()) == 6
assert ping._select().count(',') == 3
assert ping._groupby() == 'Iccid, NodeId'

assert ping in _all_tables() and gps in _all_tables()

assert _check_table(ping) is ping
assert _check_table('ping') is ping

try: _check_table('INVALID')
except ValueError: pass
else: assert False
