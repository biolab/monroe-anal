from contextlib import contextmanager

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider


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
    _kwargs = kwargs


# Set default variables
_contact_points, _port, _username, _password, _keyspace, _kwargs = (None,) * 6
set_connection_params()


@contextmanager
def new_session() -> Session:
    """
    Yields
    ------
    cassandra.cluster.Session
        A new Cassandra session

    Examples
    --------
    >>> with new_session() as session:
    ...     session.execute('...')
    """
    cluster = Cluster(contact_points=_contact_points,
                      port=_port,
                      auth_provider=PlainTextAuthProvider(username=_username,
                                                          password=_password),
                      **_kwargs)
    session = cluster.connect(_keyspace)
    yield session
    cluster.shutdown()
