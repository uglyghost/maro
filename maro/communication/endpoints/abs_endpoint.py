# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

# native lib
import os
from abc import ABC, abstractmethod
from typing import Tuple

# private package
from maro.utils import Logger

from ..utils import PeerFinder, default_params


class AbsEndpoint(ABC):
    """Wrapper for one or more ZMQ sockets to serve as a communication endpoint in distributed applications.

    Args:
        group (str): Name of the communicating group to which this wrapper belongs. This will be used
            as the hash key when registering itself to and getting peer addresses from the Redis server.
        name (str): Unique identifier for this wrapper within the ``group`` namespace.    
        protocol (str): The underlying transport-layer protocol for transferring messages. Defaults to "tcp".
        logger: The logger instance or DummyLogger. Defaults to DummyLogger().
    """

    def __init__(
        self,
        group: str,
        name: str,
        protocol: str = default_params.zmq.protocol,
        redis_address: Tuple = (default_params.redis.host, default_params.redis.port),
        initial_ping_retry_wait: int = default_params.redis.initial_ping_retry_wait,
        max_ping_retries: int = default_params.redis.max_retries,
        log_dir: str = os.getcwd()
    ):
        self.logger = Logger(name, dump_folder=log_dir)
        self._group = group
        self._name = name
        self._protocol = protocol

        self.peer_finder = PeerFinder(
            host=redis_address[0],
            port=redis_address[1],
            initial_ping_retry_wait=initial_ping_retry_wait,
            max_ping_retries=max_ping_retries
        )

    @property
    def group(self):
        return self._group

    @property
    def name(self):
        return self._name

    @property
    def protocol(self):
        return self._protocol

    @abstractmethod
    def receive(self):
        raise NotImplementedError

    def send(self, *args):
        raise NotImplementedError
