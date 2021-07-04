# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
import pickle
import socket
from typing import Tuple

import zmq

# private package
from maro.utils.exception.communication_exception import SendError

from ..utils import Signal, default_params
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
        group: str,
        name: str,
        num_workers: int,
        protocol: str = default_params.zmq.protocol,
        recv_timeout: int = default_params.zmq.receive_timeout,
        redis_address: Tuple = (default_params.redis.host, default_params.redis.port),
        initial_redis_connect_retry_interval: int = default_params.redis.initial_retry_interval,
        max_redis_connect_retries: int = default_params.redis.max_retries,
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
        self._name = name
        self._ip_address = socket.gethostbyname(socket.gethostname())

        self._recv_timeout = recv_timeout

        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.setsockopt(zmq.RCVTIMEO, self._recv_timeout)

        port = self._socket.bind_to_random_port(f"{self._protocol}://*")
        self._address = f"{self._protocol}://{self._ip_address}:{port}"
        self.logger.info(f"Ready to communicate at {self._address}.")

        # Initialize connection to the redis server.
        self.peer_finder.register(self._address)

        self._workers = []
        while len(self._workers) != num_workers:
            worker_id, _, content = self.receive()
            if content == Signal.ONBOARD:
                self._workers.append(worker_id)
                self.logger.info(f"{worker_id} onboard")

    @property
    def address(self):
        return self._address

    @property
    def workers(self):
        return self._workers

    def receive(self):
        try:
            peer_id, _, payload = self._socket.recv_multipart()
            return pickle.loads(payload), peer_id
        except zmq.ZMQError:
            self.logger.error(f"Receive timed out")

    def send(self, peer_id, msg):
        try:
            self._socket.send_multipart([peer_id, b"", pickle.dumps(msg)])
        except Exception as e:
            raise SendError(f"Failed to send to {peer_id} due to: {e}")

    def exit(self):
        """Tell the remote trainers to exit."""
        for worker_id in self._workers:
            self.send(worker_id, Signal.EXIT)

        # Avoid hanging infinitely
        self._context.setsockopt(zmq.LINGER, 0)

        # Close all sockets
        self._socket.close()
        self._context.term()

        self.logger.info(f"{self._name} Exiting...")
