from monroe.util import aslist


class _TableMeta(type):
    def __new__(mcs, name, bases, attrs):
        __attrs = {k: (((attrs.get('__table_name__') or name) + '.' + (v or k))
                       if not k.startswith('_') else v)
                   for k, v in attrs.items()}
        __attrs['__members__'] = [k for k in attrs if not k.startswith('_')]
        return super().__new__(mcs, name, bases, __attrs)

    def __repr__(cls):
        return getattr(cls, '__table_name__') or cls.__name__


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

_CATEGORICAL_COLUMNS = {
    'NodeId',
    'Operator',
    'Iccid',
    'Host',
    'EventType',
    'Message',
    'InterfaceName',
    'InternalInterface',
    'Cid',
    'DeviceMode',
    'DeviceState',
    'IpAddress',
    'ImsiMccMnc',
    'Frequency',
    'ErrorCode',
    'targetdomainname',
    'IpDst',
    'RTTSection'
}

_CAST_TYPES = {
    # Cast ill-typed columns into more reasonable types
    # From https://cassandra.apache.org/doc/latest/cql/functions.html#cast
    'Timestamp': 'double',
    'ImsiMccMnc': 'text',
    'Frequency': 'text',
    # 'NodeId': 'text',
}
# Returned columns are renamed. Lets revert rename them to manageable values.
_CAST_RENAMES = {'cast_{}_as_{}'.format(col.lower(), cast_type): col.lower()
                 for col, cast_type in _CAST_TYPES.items()}


class ping(_Table):
    __table_name__ = 'monroe_exp_ping'
    NodeId = Timestamp = Operator = Iccid = Host = Rtt = ''


class gps(_Table):
    __table_name__ = 'monroe_meta_device_gps'
    NodeId = Timestamp = Latitude = Longitude = Altitude = Speed = SatelliteCount = ''


class sensor(_Table):
    __table_name__ = 'monroe_meta_node_sensor'
    NodeId = Timestamp = Running = Cpu = Id = Current = Total = User = \
        Apps = Free = Swap = usb0 = usb1 = usb2 = ''


class event(_Table):
    __table_name__ = 'monroe_meta_node_event'
    NodeId = Timestamp = EventType = Message = ''


class modem(_Table):
    __table_name__ = 'monroe_meta_device_modem'
    NodeId = Timestamp = InternalInterface = Cid = DeviceMode = \
        DeviceState = Ecio = Iccid = IpAddress = ImsiMccMnc = Operator = \
        Rsrq = Rssi = Frequency = ''

    def __transform__(self, df):
        # from https://github.com/MONROE-PROJECT/data-exporter
        if 'devicemode' in df:
            df.devicemode.replace(
                dict(enumerate(('unknown', 'disconnected', 'no_service', '2G', '3G', 'LTE'), 1)),
                inplace=True)
        if 'devicestate' in df:
            df.devicestate.replace(
                dict(enumerate(('unknown', 'registered', 'unregistered', 'connected', 'disconnected'))),
                inplace=True)
        return df


class http(_Table):
    __table_name__ = 'monroe_exp_http_download'
    NodeId = Timestamp = Operator = Iccid = DownloadTime = Host = Speed = ErrorCode = ''


class traceroute(_Table):
    __table_name__ = 'monroe_exp_simple_traceroute'
    NodeId = Timestamp = targetdomainname = InterfaceName = IpDst = numberOfHops = RTTSection = ''


# Hide .mro() member by exposing instances instead of types
ping = ping()
gps = gps()
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
                        T.__class__.__name__ == table or
                        T.__class__.__table_name__ == table))
    except StopIteration:
        raise ValueError('Unknown MONROE table: {}'.format(table))


def _check_columns(table, columns):
    table = _check_table(table)
    columns = aslist(columns, str)

    if str(table) in columns:
        columns = ['*']

    new_columns = []
    select_all = False
    for col in columns:

        # Deal with * later
        if col == '*':
            select_all = True
            break

        for tcol in table.__members__:
            if tcol.lower() == col.rpartition('.')[-1].lower():
                new_columns.append(tcol)
                break
        else:
            raise ValueError("Unknown column '{}' for table '{}'".format(col, table))

    if select_all or len(columns) == 0:
        for tcol in table.__members__:
            if tcol not in new_columns:
                new_columns.append(tcol)

    return new_columns


# Unit tests

assert str(ping) == ping.__table_name__
assert str(ping.__class__) == ping.__table_name__
assert str(ping.NodeId) == ping.__table_name__ + '.' + 'NodeId'
assert str(ping.__class__.NodeId) == ping.__table_name__ + '.' + 'NodeId'

assert ping in _all_tables() and gps in _all_tables()

assert _check_table(ping) is ping
assert _check_table('ping') is ping

try: _check_table('INVALID')
except ValueError: pass
else: assert False

assert 'NodeId' in ping.__members__
assert len(_check_columns(ping, '*')) == len(ping.__members__) == 6
assert len(_check_columns(ping, [])) == len(ping.__members__) == 6
assert len(_check_columns(ping, ['*', 'NodeId'])) == len(ping.__members__)
assert len(_check_columns('ping', '*')) == len(ping.__members__)
assert _check_columns(ping, 'NodeId') == ['NodeId']
assert _check_columns(ping, 'nodeid') == ['NodeId']
assert _check_columns(ping, ping.NodeId) == ['NodeId']
assert _check_columns(ping, [ping]) == _check_columns(ping, '*')
