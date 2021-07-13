# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import json
import time
from typing import List, Union

import redis

from maro.utils.exception.communication_exception import PeerDicoveryFailure, RedisConnectionError


class PeerFinder:
    def __init__(
        self,
        group: str,
        name: str,
        host: str = "localhost",
        port: int = 6379,
        initial_ping_retry_wait: int = 1.0,
        max_ping_retries: int = 10
    ):
        # Initialize connection to the redis server.
        self._conn = redis.Redis(host=host, port=port, socket_keepalive=True)

        next_retry = initial_ping_retry_wait
        ping_successful = False
        for _ in range(max_ping_retries):
            try:
                self._conn.ping()
                ping_successful = True
            except Exception:
                time.sleep(next_retry)
                next_retry *= 2

        if not ping_successful:
            raise RedisConnectionError(f"{self._name} failed to connect to the redis server.")

        self._group = group
        self._name = name

    @property
    def group(self):
        return self._group

    @property
    def name(self):
        return self._name

    def register(self, address):
        # upload the endpoint address to the Redis server with group as the hash key.
        self._conn.hset(self._group, self._name, json.dumps(address))

    def discover(self, peers: Union[str, List[str]], initial_retry_wait: float = 1.0, max_retries: int = 10):
        # peer discovery
        if isinstance(peers, str):
            peers = [peers]

        next_retry = initial_retry_wait
        for _ in range(max_retries):
            addresses = self._conn.hmget(self._group, peers)
            if any(not addr for addr in addresses):
                time.sleep(next_retry)
                next_retry *= 2
            else:
                return [json.loads(addr) for addr in addresses] if len(addresses) > 1 else json.loads(addresses[0])

        raise PeerDicoveryFailure(f"{self._name} failed to discover all peers.")
