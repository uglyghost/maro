# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import json
import time

def ping(conn, initial_retry_interval: float = 1.0, max_retries: int = 10):
    next_retry = initial_retry_interval
    for _ in range(max_retries):
        try:
            conn.ping()
            return True
        except Exception:
            time.sleep(next_retry)
            next_retry *= 2

    return False


def push(conn, group, name, address):
    # upload the endpoint address to the Redis server with group as the hash key.
    conn.hset(group, name, json.dumps(address))


def discover_peers(conn, group: str, peers: list, initial_retry_interval: float = 1.0, max_retries: int = 10):
    # peer discovery
    peer_address = {}

    next_retry = initial_retry_interval
    for _ in range(max_retries):
        addresses = conn.hmget(group, peers)
        if any(not addr for addr in addresses):
            time.sleep(next_retry)
            next_retry *= 2
        else:
            addresses = [json.loads(addr) for addr in addresses]
            peer_address = dict(zip(peers, addresses))
            break
    
    return peer_address
