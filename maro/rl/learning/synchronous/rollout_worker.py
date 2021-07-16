# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from multiprocessing.connection import Connection
from os import getcwd
from typing import Callable

from maro.communication.endpoints import SyncWorkerEndpoint
from maro.communication.utils import Signal
from maro.rl.utils import MsgTag
from maro.utils import Logger, set_seeds

from ..agent_wrapper import AgentWrapper
from ..env_wrapper import AbsEnvWrapper


def rollout_worker_process(
    index: int,
    conn: Connection,
    create_env_wrapper_func: Callable[[], AbsEnvWrapper],
    create_agent_wrapper_func: Callable[[], AgentWrapper],
    create_eval_env_wrapper_func: Callable[[], AbsEnvWrapper] = None,
    log_dir: str = getcwd()
):
    """Roll-out worker process that can be spawned by a ``MultiProcessRolloutManager``.

    Args:
        index (int): Index for the worker process. This is used for bookkeeping by the parent manager process.
        conn (Connection): Connection end for exchanging messages with the manager process.
        create_env_wrapper_func (Callable): Function to create an environment wrapper for training data collection.
            The function should take no parameters and return an environment wrapper instance.
        create_agent_wrapper_func (Callable): Function to create a decision generator for interacting with
            the environment. The function should take no parameters and return a ``AgentWrapper`` instance.
        create_env_wrapper_func (Callable): Function to create an environment wrapper for evaluation. The function
            should take no parameters and return an environment wrapper instance. If this is None, the training
            environment wrapper will be used for evaluation. Defaults to None.
        log_dir (str): Directory to store logs in. Defaults to the current working directory.
    """
    set_seeds(index)
    env_wrapper = create_env_wrapper_func()
    eval_env_wrapper = env_wrapper if not create_eval_env_wrapper_func else create_eval_env_wrapper_func()
    agent_wrapper = create_agent_wrapper_func()
    logger = Logger("ROLLOUT_WORKER", dump_folder=log_dir)

    def collect(msg):
        ep, segment = msg["episode"], msg["segment"]
        # set policy states
        agent_wrapper.set_policy_states(msg["policy"])

        # update exploration parameters
        agent_wrapper.explore()
        if msg["exploration_step"]:
            agent_wrapper.exploration_step()

        if env_wrapper.state is None:
            logger.info(f"Roll-out episode {ep}")
            env_wrapper.reset()
            env_wrapper.collect()
            env_wrapper.start()  # get initial state

        starting_step_index = env_wrapper.step_index + 1
        steps_to_go = float("inf") if msg["num_steps"] == -1 else msg["num_steps"]
        while env_wrapper.state and steps_to_go > 0:
            action = agent_wrapper.choose_action(env_wrapper.state)
            env_wrapper.step(action)
            steps_to_go -= 1

        logger.info(
            f"Roll-out finished (episode {ep}, segment {segment}, "
            f"steps {starting_step_index} - {env_wrapper.step_index})"
        )

        return_info = {
            "worker_index": index,
            "episode_end": not env_wrapper.state,
            "experiences": agent_wrapper.get_batch(env_wrapper),
            "tracker": env_wrapper.tracker,
            "num_steps": env_wrapper.step_index - starting_step_index + 1
        }

        conn.send(return_info)

    def evaluate(msg):
        logger.info("Evaluating...")
        agent_wrapper.set_policy_states(msg["policy"])
        agent_wrapper.exploit()
        eval_env_wrapper.reset()
        eval_env_wrapper.evaluate()
        eval_env_wrapper.start()  # get initial state
        while eval_env_wrapper.state:
            action = agent_wrapper.choose_action(eval_env_wrapper.state)
            eval_env_wrapper.step(action)

        conn.send({"worker_id": index, "tracker": eval_env_wrapper.tracker})

    while True:
        msg = conn.recv()
        if msg["type"] == "collect":
            collect(msg)
        elif msg["type"] == "evaluate":
            evaluate(msg)
        elif msg["type"] == "quit":
            break


def rollout_worker_node(
    group: str,
    worker_id: int,
    env_wrapper: AbsEnvWrapper,
    agent_wrapper: AgentWrapper,
    eval_env_wrapper: AbsEnvWrapper = None,
    endpoint_kwargs: dict = {},
    log_dir: str = getcwd()
):
    """Roll-out worker process that can be launched on separate computation nodes.

    Args:
        group (str): Group name for the roll-out cluster, which includes all roll-out workers and a roll-out manager
            that manages them.
        worker_idx (int): Worker index. The worker's ID in the cluster will be "ROLLOUT_WORKER.{worker_idx}".
            This is used for bookkeeping by the parent manager.
        env_wrapper (AbsEnvWrapper): Environment wrapper for training data collection.
        agent_wrapper (AgentWrapper): Agent wrapper to interact with the environment wrapper.
        eval_env_wrapper (AbsEnvWrapper): Environment wrapper for evaluation. If this is None, the training
            environment wrapper will be used for evaluation. Defaults to None.
        endpoint_kwargs: Keyword parameters for the internal ``Proxy`` instance. See ``Proxy`` class
            for details. Defaults to the empty dictionary.
        log_dir (str): Directory to store logs in. Defaults to the current working directory.
    """
    worker_id = f"ROLLOUT_WORKER.{worker_id}"
    logger = Logger(worker_id, dump_folder=log_dir)
    eval_env_wrapper = env_wrapper if not eval_env_wrapper else eval_env_wrapper
    endpoint = SyncWorkerEndpoint(group, worker_id, "ROLLOUT_MANAGER", **endpoint_kwargs)

    def collect(msg):
        ep, segment = msg["episode"], msg["segment"]

        # set policy states
        agent_wrapper.set_policy_states(msg["policy_state"])

        # set exploration parameters
        agent_wrapper.explore()
        if msg["exploration_step"]:
            agent_wrapper.exploration_step()

        if env_wrapper.state is None:
            logger.info(f"Roll-out episode {msg['episode']}")
            env_wrapper.reset()
            env_wrapper.collect()
            env_wrapper.start()  # get initial state

        starting_step_index = env_wrapper.step_index + 1
        steps_to_go = float("inf") if msg["num_steps"] == -1 else msg["num_steps"]
        while env_wrapper.state and steps_to_go > 0:
            action = agent_wrapper.choose_action(env_wrapper.state)
            env_wrapper.step(action)
            steps_to_go -= 1

        logger.info(
            f"Roll-out finished (episode {ep}, segment {segment}, "
            f"steps {starting_step_index} - {env_wrapper.step_index})"
        )

        endpoint.send({
            "type": MsgTag.COLLECT_DONE,
            "episode": ep,
            "segment": segment,
            "version": msg["version"],
            "experiences": agent_wrapper.get_batch(env_wrapper),
            "tracker": env_wrapper.tracker,
            "num_steps": env_wrapper.step_index - starting_step_index + 1,
            "end_of_episode": not env_wrapper.state
        })

    def evaluate(msg):
        logger.info("Evaluating...")
        agent_wrapper.set_policy_states(msg["policy_state"])
        agent_wrapper.exploit()
        eval_env_wrapper.reset()
        eval_env_wrapper.evaluate()
        eval_env_wrapper.start()  # get initial state
        while eval_env_wrapper.state:
            action = agent_wrapper.choose_action(eval_env_wrapper.state)
            eval_env_wrapper.step(action)

        endpoint.send({"type": MsgTag.EVAL_DONE, "tracker": eval_env_wrapper.tracker, "episode": msg["episode"]})

    """
    The event loop handles 3 types of messages from the roll-out manager:
        1)  COLLECT, upon which the agent-environment simulation will be carried out for a specified number of steps
            and the collected experiences will be sent back to the roll-out manager;
        2)  EVAL, upon which the policies contained in the message payload will be evaluated for the entire
            duration of the evaluation environment.
        3)  EXIT, upon which it will break out of the event loop and the process will terminate.

    """
    while True:
        msg = endpoint.receive()
        if msg == Signal.EXIT:
            endpoint.close()
            logger.info(f"{endpoint.name} exiting...")
            break

        if msg["type"] == MsgTag.COLLECT:
            collect(msg)
        elif msg["type"] == MsgTag.EVAL:
            evaluate(msg)
