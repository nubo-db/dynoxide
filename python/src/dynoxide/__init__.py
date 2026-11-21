"""Start a Dynoxide server, a DynamoDB-compatible engine, from Python.

    import boto3
    import dynoxide

    with dynoxide.start() as server:
        client = boto3.client("dynamodb", **server.client_kwargs())
        client.list_tables()

The pytest plugin in ``dynoxide.pytest_plugin`` loads on its own and gives
tests ``dynoxide``, ``dynamodb_client``, ``dynamodb_resource`` and
``dynoxide_clean`` fixtures.
"""

from ._binary import binary_path
from ._errors import DynoxideError, PortInUseError
from ._server import Server, start
from ._version import ENGINE_VERSION, __version__

__all__ = [
    "ENGINE_VERSION",
    "DynoxideError",
    "PortInUseError",
    "Server",
    "__version__",
    "binary_path",
    "start",
]
