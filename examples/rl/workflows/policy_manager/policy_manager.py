# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import sys
from os import getenv
from os.path import dirname, realpath

from maro.rl.learning import DistributedPolicyManager, SimplePolicyManager
from maro.utils import Logger

workflow_dir = dirname(dirname(realpath(__file__)))  # template directory
if workflow_dir not in sys.path:
    sys.path.insert(0, workflow_dir)

from general import log_dir, rl_policy_func_index

def get_policy_manager():
<<<<<<< HEAD
    train_mode = config["policy_manager"]["train_mode"]
    num_trainers = config["policy_manager"]["num_trainers"]
    policy_dict = {name: func() for name, func in rl_policy_func_index.items()}
    if train_mode == "single-process":
        return LocalPolicyManager(
            policy_dict,
            update_trigger=update_trigger,
            warmup=warmup,
            log_dir=log_dir
        )
    if train_mode == "multi-process":
        return MultiProcessPolicyManager(
            policy_dict,
            num_trainers,
            rl_policy_func_index,
            update_trigger=update_trigger,
            warmup=warmup,
            log_dir=log_dir
        )
    if train_mode == "multi-node":
        return MultiNodePolicyManager(
            policy_dict,
            config["policy_manager"]["train_group"],
            num_trainers,
            update_trigger=update_trigger,
            warmup=warmup,
            endpoint_kwargs={"redis_address": (config["redis"]["host"], config["redis"]["port"])},
=======
    logger = Logger("policy manager creator")
    manager_type = getenv("POLICYMANAGERTYPE", default="simple")
    parallel = int(getenv("PARALLEL", default=0))
    if manager_type == "simple":
        return SimplePolicyManager(rl_policy_func_index, parallel=parallel, log_dir=log_dir)

    group = getenv("LEARNGROUP", default="learn")
    num_hosts = int(getenv("NUMHOSTS", default=5))
    if manager_type == "distributed":
        policy_manager = DistributedPolicyManager(
            list(rl_policy_func_index.keys()), group, num_hosts,
            proxy_kwargs={
                "redis_address": (getenv("REDISHOST", default="maro-redis"), int(getenv("REDISPORT", default=6379))),
                "max_peer_discovery_retries": 50    
            },
>>>>>>> v0.2_rl_refinement
            log_dir=log_dir
        )
        logger.info("Distributed policy manager created")
        return policy_manager

    raise ValueError(f"Unsupported policy manager type: {manager_type}. Supported modes: simple, distributed")
