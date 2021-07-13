# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from maro.utils import convert_dottable

default_params = convert_dottable({
    "fault_tolerant": False,
    "delay_for_slow_joiner": 3,
    "peer_discovery": {
        "initial_retry_wait": 0.1,
        "max_retries": 10
    },
    "redis": {
        "host": "localhost",
        "port": 6379,
        "initial_ping_retry_wait": 0.1,
        "max_retries": 10
    },
    "zmq": {
        "protocol": "tcp",
        "send_timeout": -1,
        "max_send_retries": 5,
        "receive_timeout": -1
    }
})
