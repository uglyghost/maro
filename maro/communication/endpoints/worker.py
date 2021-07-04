# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
import pickle
from typing import Tuple

import zmq

from ..utils import Signal, default_params
from .abs_endpoint import AbsEndpoint


class SyncWorkerEndpoint(AbsEndpoint):
    """Wrapper for one or more ZMQ sockets to serve as a communication endpoint in distributed applications.

    Args:
        group (str): Name of the communicating group to which this endpoint belongs. This will be used
            as the hash key when registering itself to and getting peer addresses from the Redis server.
        name (str): Unique identifier for this endpoint within the ``group`` namespace.
        protocol (str): The underlying transport-layer protocol for transferring messages. Defaults to "tcp".
        send_timeout (int): The timeout in milliseconds for sending message. If -1, no timeout (infinite).
            Defaults to -1.
        recv_timeout (int): The timeout in milliseconds for receiving message. If -1, no timeout (infinite).
            Defaults to -1.
        logger: The logger instance or DummyLogger. Defaults to DummyLogger().
    """

    def __init__(
        self,
        group: str,
        name: str,
        manager: str,
        protocol: str = default_params.zmq.protocol,
        send_timeout: int = default_params.zmq.send_timeout,
        redis_address: Tuple = (default_params.redis.host, default_params.redis.port),
        initial_redis_connect_retry_interval: int = default_params.redis.initial_retry_interval,
        max_redis_connect_retries: int = default_params.redis.max_retries,
        initial_peer_discovery_retry_interval: int = default_params.peer_discovery.initial_retry_interval,
        max_peer_discovery_retries: int = default_params.peer_discovery.max_retries,
        log_dir: str = os.getcwd()
    ):
        super().__init__(
            group, name,
            protocol=protocol,
            redis_address=redis_address,
            initial_redis_connect_retry_interval=initial_redis_connect_retry_interval,
            max_redis_connect_retries=max_redis_connect_retries,
            log_dir=log_dir
        )

        self._context = zmq.Context()
        self._send_timeout = send_timeout
        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.SNDTIMEO, self._send_timeout)
        self._socket.setsockopt(zmq.IDENTITY, bytes(self._name))

        # peer discovery
        self._socket.connect(
            self.peer_finder.discover(
                manager,
                initial_retry_interval=initial_peer_discovery_retry_interval,
                max_retries=max_peer_discovery_retries
            )
        )

        self.send(Signal.ONBOARD)

    @property
    def peers(self):
        return self._peers

    def receive(self):
        return pickle.loads(self._socket.recv())

    def send(self, msg):
        self._socket.send(pickle.dumps(msg))

    def close(self):
        """Close ZMQ context and sockets."""
        # Avoid hanging infinitely
        self._context.setsockopt(zmq.LINGER, 0)

        # Close all sockets
        self._socket.close()
        self._context.term()
