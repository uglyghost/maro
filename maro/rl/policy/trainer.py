# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import time
from multiprocessing.connection import Connection
from os import getcwd
from typing import Callable, Dict

from maro.communication.endpoints import SyncWorkerEndpoint
from maro.communication.utils import Signal
from maro.rl.utils import MsgTag
from maro.utils import Logger


def trainer_process(
    trainer_id: int,
    conn: Connection,
    create_policy_func_dict: Dict[str, Callable],
    initial_policy_states: dict,
    log_dir: str = getcwd()
):
    """Policy trainer process which can be spawned by a ``MultiProcessPolicyManager``.

    Args:
        trainer_id (int): Integer trainer ID.
        conn (Connection): Connection end for exchanging messages with the manager process.
        create_policy_func_dict (dict): A dictionary mapping policy names to functions that create them. The policy
            creation function should have exactly one parameter which is the policy name and return an ``AbsPolicy``
            instance.
        log_dir (str): Directory to store logs in. Defaults to the current working directory.
    """
    policy_dict = {policy_name: func() for policy_name, func in create_policy_func_dict.items()}
    logger = Logger("TRAINER", dump_folder=log_dir)
    for name, state in initial_policy_states.items():
        policy_dict[name].set_state(state)
        logger.info(f"{trainer_id} initialized policy {name}")

    while True:
        msg = conn.recv()
        if msg["type"] == "train":
            t0 = time.time()
            for name, exp in msg["experiences"].items():
                policy_dict[name].store(exp)
                policy_dict[name].learn()
            logger.debug(f"total policy update time: {time.time() - t0}")
            conn.send({"policy": {name: policy_dict[name].get_state() for name in msg["experiences"]}})
        elif msg["type"] == "quit":
            break


def trainer_node(
    group: str,
    trainer_idx: int,
    create_policy_func_dict: Dict[str, Callable],
    endpoint_kwargs: dict = {},
    log_dir: str = getcwd()
):
    """Policy trainer process that can be launched on separate computation nodes.

    Args:
        group (str): Group name for the training cluster, which includes all trainers and a training manager that
            manages them.
        trainer_idx (int): Integer trainer index. The trainer's ID in the cluster will be "TRAINER.{trainer_idx}".
        create_policy_func_dict (dict): A dictionary mapping policy names to functions that create them. The policy
            creation function should have exactly one parameter which is the policy name and return an ``AbsPolicy``
            instance.
        endpoint_kwargs: Keyword parameters for the internal ``Proxy`` instance. See ``Proxy`` class
            for details. Defaults to the empty dictionary.
        log_dir (str): Directory to store logs in. Defaults to the current working directory.
    """
    trainer_id = f"TRAINER.{trainer_idx}"
    logger = Logger(trainer_id, dump_folder=log_dir)
    policy_dict = {}
    endpoint = SyncWorkerEndpoint(group, trainer_id, "POLICY_MANAGER", **endpoint_kwargs) 

    while True:
        msg = endpoint.receive()
        if msg == Signal.EXIT:
            endpoint.close()
            logger.info(f"{endpoint.name} exiting...")
            break

        if msg["type"] == MsgTag.INIT_POLICY_STATE:
            for name, state in msg["body"].items():
                policy_dict[name] = create_policy_func_dict[name]()
                policy_dict[name].set_state(state)
                logger.info(f"{trainer_id} initialized policy {name}")
            endpoint.send({"type": MsgTag.INIT_POLICY_STATE_DONE})
        elif msg["type"] == MsgTag.LEARN:
            t0 = time.time()
            for name, exp in msg["body"].items():
                policy_dict[name].store(exp)
                policy_dict[name].learn()

            logger.info(f"total policy update time: {time.time() - t0}")
            policy_states = {name: policy_dict[name].get_state() for name in msg["body"]}
            endpoint.send({"type": MsgTag.POLICY_STATE, "body": policy_states})
