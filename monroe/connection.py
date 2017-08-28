import atexit
from contextlib import contextmanager

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RetryPolicy


class _ConstantRetryPolicy(RetryPolicy):
    # TODO check if this works on reset connections
    def on_read_timeout(self, *args, **kwargs):
        return RetryPolicy.RETRY, None


def set_connection_params(contact_points: str or [str]='127.0.0.1',
                          port: int=9042,
                          username: str='monroedb',
                          password: str='monroedb_pass',
                          keyspace: str='monroe',
                          **kwargs):
    global _contact_points, _port, _username, _password, _keyspace, _kwargs

    if isinstance(contact_points, str):
        contact_points = [contact_points]

    _contact_points = contact_points or ['127.0.0.1']
    _port = port
    _username = username
    _password = password
    _keyspace = keyspace

    _kwargs = dict(idle_heartbeat_timeout=10,
                   default_retry_policy=_ConstantRetryPolicy())
    _kwargs.update(kwargs)


# Set default variables
_contact_points = _port = _username = _password = _keyspace = _kwargs = None
set_connection_params()

_session = _cluster = None

@contextmanager
def get_session() -> Session:
    """
    Yields
    ------
    cassandra.cluster.Session
        A new Cassandra session

    Examples
    --------
    >>> with get_session() as session:
    ...     session.execute('...')
    """
    global _session, _cluster
    if _cluster is None or _cluster.is_shutdown:
        _cluster = Cluster(contact_points=_contact_points,
                          port=_port,
                          auth_provider=PlainTextAuthProvider(username=_username,
                                                              password=_password),
                          **_kwargs)
    if _session is None or _session.is_shutdown:
        _session = _cluster.connect(_keyspace)
        _session.default_timeout = 30

    yield _session


atexit.register(lambda: _cluster is not None and _cluster.shutdown())
