# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from os import getcwd
from typing import Callable

from maro.communication.endpoints import SyncWorkerEndpoint
from maro.communication.utils import Signal
from maro.rl.wrappers import AbsEnvWrapper, AgentWrapper
from maro.rl.utils import MsgTag
from maro.utils import Logger 

from .common import get_rollout_finish_msg


class EnvironmentSampler:
    """Simulation data collector and policy evaluator.

    Args:
        get_env_wrapper (Callable): Function to create an environment wrapper for collecting training data. The function
            should take no parameters and return an environment wrapper instance.
        get_agent_wrapper (Callable): Function to create an agent wrapper that interacts with the environment wrapper.
            The function should take no parameters and return a ``AgentWrapper`` instance.
        get_eval_env_wrapper (Callable): Function to create an environment wrapper for evaluation. The function should
            take no parameters and return an environment wrapper instance. If this is None, the training environment
            wrapper will be used for evaluation in the worker processes. Defaults to None.
    """
    def __init__(
        self,
        get_env_wrapper: Callable[[], AbsEnvWrapper],
        get_agent_wrapper: Callable[[], AgentWrapper],
        get_eval_env_wrapper: Callable[[], AbsEnvWrapper] = None
    ):
        self.env = get_env_wrapper()
        self.eval_env = get_env_wrapper() if get_eval_env_wrapper else self.env
        self.agent = get_agent_wrapper()

    def sample(self, policy_state_dict: dict = None, num_steps: int = -1, exploration_step: bool = False):
        # set policy states
        if policy_state_dict:
            self.agent.set_policy_states(policy_state_dict)

        # set exploration parameters
        self.agent.explore()
        if exploration_step:
            self.agent.exploration_step()

        if self.env.state is None:
            self.env.reset()
            self.env.replay = True
            self.env.start()  # get initial state

        starting_step_index = self.env.step_index + 1
        steps_to_go = float("inf") if num_steps == -1 else num_steps
        while self.env.state and steps_to_go > 0:
            action = self.agent.choose_action(self.env.state)
            self.env.step(action)
            steps_to_go -= 1

        return {
            "rollout_info": self.agent.get_rollout_info(self.env.get_trajectory()),
            "step_range": (starting_step_index, self.env.step_index),
            "tracker": self.env.tracker,
            "end_of_episode": not self.env.state,
            "exploration_params": self.agent.exploration_params
        }

    def test(self, policy_state_dict: dict = None):
        if policy_state_dict:
            self.agent.set_policy_states(policy_state_dict)
        self.agent.exploit()
        self.eval_env.reset()
        self.eval_env.replay = False
        self.eval_env.start()  # get initial state
        while self.eval_env.state:
            action = self.agent.choose_action(self.eval_env.state)
            self.eval_env.step(action)

        return self.eval_env.tracker

    def worker(self, group: str, index: int, endpoint_kwargs: dict = {}, log_dir: str = getcwd()):
        """Roll-out worker process that can be launched on separate computation nodes.

        Args:
            group (str): Group name for the roll-out cluster, which includes all roll-out workers and a roll-out manager
                that manages them.
            index (int): Worker index. The worker's ID in the cluster will be "ROLLOUT_WORKER.{worker_idx}".
                This is used for bookkeeping by the parent manager.
            endpoint_kwargs: Keyword parameters for the internal ``SyncWorkerEndpoint`` instance. See
                ``SyncWorkerEndpoint`` class for details. Defaults to the empty dictionary.
            log_dir (str): Directory to store logs in. Defaults to the current working directory.
        """
        worker_id = f"ROLLOUT_WORKER.{index}"
        endpoint = SyncWorkerEndpoint(group, worker_id, "ROLLOUT_MANAGER", **endpoint_kwargs)
        logger = Logger(worker_id, dump_folder=log_dir)

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

            if msg["type"] == MsgTag.SAMPLE:
                ep = msg["episode"]
                result = self.sample(
                    policy_state_dict=msg["policy_state"],
                    num_steps=msg["num_steps"],
                    exploration_step=msg["exploration_step"]
                )
                logger.info(
                    get_rollout_finish_msg(ep, result["step_range"], exploration_params=result["exploration_params"])
                )
                endpoint.send({
                    "type": MsgTag.SAMPLE_DONE,
                    "episode": ep,
                    "segment": msg["segment"],
                    "version": msg["version"],
                    "rollout_info": result["rollout_info"],
                    "step_range": result["step_range"],
                    "tracker": result["tracker"],
                    "end_of_episode": result["end_of_episode"]
                })
            elif msg.tag == MsgTag.TEST:
                tracker = self.test(msg["policy_state"])
                logger.info("Testing complete")
                endpoint.send({"type": MsgTag.TEST_DONE, "tracker": tracker, "episode": ep})

    def actor(
        self,
        group: str,
        index: int,
        num_episodes: int,
        num_steps: int = -1,
        proxy_kwargs: dict = {},
        log_dir: str = getcwd()
    ):
        """Controller for single-threaded learning workflows.

        Args:
            group (str): Group name for the cluster that includes the server and all actors.
            index (int): Integer actor index. The actor's ID in the cluster will be "ACTOR.{actor_idx}".
            num_episodes (int): Number of training episodes. Each training episode may contain one or more
                collect-update cycles, depending on how the implementation of the roll-out manager.
            num_steps (int): Number of environment steps to roll out in each call to ``collect``. Defaults to -1, in
                which case the roll-out will be executed until the end of the environment.
            proxy_kwargs: Keyword parameters for the internal ``Proxy`` instance. See ``Proxy`` class
                for details. Defaults to the empty dictionary.
            log_dir (str): Directory to store logs in. A ``Logger`` with tag "LOCAL_ROLLOUT_MANAGER" will be created at init
                time and this directory will be used to save the log files generated by it. Defaults to the current working
                directory.
        """
        pass