# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

# native lib
import pickle
import socket
import sys
from abc import ABC
from typing import Tuple

# third party package
import redis
import zmq

# private package
from maro.utils import DummyLogger
from maro.utils.exception.communication_exception import DriverSendError
from maro.utils.exit_code import NON_RESTART_EXIT_CODE

from ..utils import default_params, ping


class AbsEndpoint(ABC):
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
        protocol: str = default_params.zmq.protocol,
        send_timeout: int = default_params.zmq.send_timeout,
        recv_timeout: int = default_params.zmq.receive_timeout,
        redis_address: Tuple = (default_params.redis.host, default_params.redis.port),
        initial_redis_connect_retry_interval: int = default_params.redis.initial_retry_interval,
        max_redis_connect_retries: int = default_params.redis.max_retries,
        logger=DummyLogger()
    ):
        self.logger = logger
        self._group = group
        self._protocol = protocol
        self._send_timeout = send_timeout
        self._recv_timeout = recv_timeout

        self._context = zmq.Context()
        self.socket = self._context.socket(zmq_socket_type)
        self.socket.setsockopt(zmq.SNDTIMEO, self._send_timeout)
        self.socket.setsockopt(zmq.RCVTIMEO, self._recv_timeout)

        # Initialize connection to the redis server.
        self.redis_conn = redis.Redis(host=redis_address[0], port=redis_address[1], socket_keepalive=True)

        if not ping(
            self.redis_conn,
            initial_retry_interval=initial_redis_connect_retry_interval,
            max_retries=max_redis_connect_retries
        ):
            self.logger.error(f"{self._name} failed to connect to the redis server.")
            sys.exit(NON_RESTART_EXIT_CODE)

    @property
    def group(self):
        return self._group

    @property
    def protocol(self):
        return self._protocol

    def receive(self):
        try:
            peer_id, _, payload = self.socket.recv_multipart()
            return pickle.loads(payload), peer_id
        except zmq.ZMQError:
            self.logger.error(f"Receive timed out")

    def send(self, peer_id, msg):
        try:
            self.socket.send_multipart([peer_id, b"", pickle.dumps(msg)])
        except Exception as e:
            raise DriverSendError(f"Failure to send the message: {e}")

    def close(self):
        """Close ZMQ context and sockets."""
        # Avoid hanging infinitely
        self._context.setsockopt(zmq.LINGER, 0)

        # Close all sockets
        self.socket.close()
        self._context.term()
