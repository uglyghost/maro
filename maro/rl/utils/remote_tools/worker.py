# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import time
from os import getcwd
from typing import Callable, Dict

from maro.communication import Proxy
from maro.rl.utils import MsgKey, MsgTag
from maro.utils import Logger


def worker(
    group: str,
    worker_idx: int,
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
    pass
