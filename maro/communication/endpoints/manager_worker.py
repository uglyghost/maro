# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

# native lib
import socket
from typing import List, Tuple

# private package
from maro.utils import DummyLogger
from maro.utils.exception.communication_exception import NoPeerError, PeerDicoveryFailure

from ..utils import default_params, discover_peers, push
from .abs_endpoint import AbsEndpoint


class ManagerEndpoint(AbsEndpoint):
    """Wrapper for one or more ZMQ sockets to serve as a communication endpoint in distributed applications.

    Args:
        group (str): Name of the communicating group to which this wrapper belongs. This will be used
            as the hash key when registering itself to and getting peer addresses from the Redis server.
        name (str): Unique identifier for this wrapper within the ``group`` namespace.    
        protocol (str): The underlying transport-layer protocol for transferring messages. Defaults to "tcp".
        send_timeout (int): The timeout in milliseconds for sending message. If -1, no timeout (infinite).
            Defaults to -1.
        recv_timeout (int): The timeout in milliseconds for receiving message. If -1, no timeout (infinite).
            Defaults to -1.
        logger: The logger instance or DummyLogger. Defaults to DummyLogger().
    """

    def __init__(
        self,
        zmq_socket_type,
        group: str,
        name: str,
        protocol: str = default_params.zmq.protocol,
        send_timeout: int = default_params.zmq.send_timeout,
        recv_timeout: int = default_params.zmq.receive_timeout,
        redis_address: Tuple = (default_params.redis.host, default_params.redis.port),
        initial_redis_connect_retry_interval: int = default_params.redis.initial_retry_interval,
        max_redis_connect_retries: int = default_params.redis.max_retries,
        logger=DummyLogger()
    ):
        super().__init__(
            zmq_socket_type, group,
            protocol=protocol,
            send_timeout=send_timeout,
            recv_timeout=recv_timeout,
            redis_address=redis_address,
            initial_redis_connect_retry_interval=initial_redis_connect_retry_interval,
            max_redis_connect_retries=max_redis_connect_retries,
            logger=logger
        )
        self._name = name
        self._ip_address = socket.gethostbyname(socket.gethostname())

        port = self.socket.bind_to_random_port(f"{self._protocol}://*")
        self._address = f"{self._protocol}://{self._ip_address}:{port}"
        self.logger.info(f"Ready to communicate at {self._address}.")

        # Initialize connection to the redis server.
        push(self.redis_conn, self._group, self._name, self._address)

    @property
    def name(self):
        return self._name

    @property
    def address(self):
        return self._address


class WorkerEndpoint(AbsEndpoint):
    """Wrapper for one or more ZMQ sockets to serve as a communication endpoint in distributed applications.

    Args:
        group (str): Name of the communicating group to which this wrapper belongs. This will be used
            as the hash key when registering itself to and getting peer addresses from the Redis server.
        name (str): Unique identifier for this wrapper within the ``group`` namespace.    
        protocol (str): The underlying transport-layer protocol for transferring messages. Defaults to "tcp".
        send_timeout (int): The timeout in milliseconds for sending message. If -1, no timeout (infinite).
            Defaults to -1.
        recv_timeout (int): The timeout in milliseconds for receiving message. If -1, no timeout (infinite).
            Defaults to -1.
        logger: The logger instance or DummyLogger. Defaults to DummyLogger().
    """

    def __init__(
        self,
        zmq_socket_type,
        group: str,
        peers: List[str],
        protocol: str = default_params.zmq.protocol,
        send_timeout: int = default_params.zmq.send_timeout,
        recv_timeout: int = default_params.zmq.receive_timeout,
        redis_address: Tuple = (default_params.redis.host, default_params.redis.port),
        initial_redis_connect_retry_interval: int = default_params.redis.initial_retry_interval,
        max_redis_connect_retries: int = default_params.redis.max_retries,
        initial_peer_discovery_retry_interval: int = default_params.peer_discovery.initial_retry_interval,
        max_peer_discovery_retries: int = default_params.peer_discovery.max_retries,
        logger=DummyLogger()
    ):
        if not peers:
            raise NoPeerError("peers cannot be empty")

        super().__init__(
            zmq_socket_type, group,
            protocol=protocol,
            send_timeout=send_timeout,
            recv_timeout=recv_timeout,
            redis_address=redis_address,
            initial_redis_connect_retry_interval=initial_redis_connect_retry_interval,
            max_redis_connect_retries=max_redis_connect_retries,
            logger=logger
        )

        self._peers = peers

        # peer discovery
        self._peer_address = discover_peers(
            self.redis_conn, self._group, self._peers,
            initial_retry_interval=initial_peer_discovery_retry_interval,
            max_retries=max_peer_discovery_retries
        )

        if not self._peer_address:
            raise PeerDicoveryFailure(f"Could not discover all peers.")

        for addr in self._peer_address.values():
            self.socket.connect(addr)

    @property
    def peers(self):
        return self._peers
