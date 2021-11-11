# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import math

import numpy as np

from maro.rl.learning import AbsEnvSampler
from maro.simulator import Env
from maro.simulator.scenarios.hvac.common import Action
from maro.utils import Logger

from .callbacks import baseline
from .config import algorithm, env_config, reward_config, state_config
from .policies import policy_func_dict


attributes = state_config["attributes"]
agent_name = "ahu01"


class HVACEnvSampler(AbsEnvSampler):
    def __init__(
        self,
        get_env,
        get_policy_func_dict,
        agent2policy,
        get_test_env=None,
        reward_eval_delay=0,
        parallel_inference=False,
        logger: Logger=None,
    ):
        super().__init__(
            get_env,
            get_policy_func_dict,
            agent2policy,
            get_test_env=get_test_env,
            reward_eval_delay=reward_eval_delay,
            parallel_inference=parallel_inference
        )

        self._statistics = {
            key: {
                "mean": np.mean(baseline[key]),
                "min": np.min(baseline[key]),
                "max": np.max(baseline[key]),
                "range": np.max(baseline[key]) - np.min(baseline[key]),
            }
            for key in baseline.keys()
        }

        self._logger = logger
        if self._logger:
            self._logger.info(f"efficiency,kw,das_diff,dat_penalty,reward")

    def get_state(self, tick: int = None) -> dict:
        if tick is not None:
            assert tick == self.env.tick
        else:
            tick = self.env.tick

        state = self.env.snapshot_list["ahus"][tick:0:attributes]

        if state_config["normalize"]:
            for i, key in enumerate(attributes):
                state[i] = (state[i] - self._statistics[key]["min"]) / self._statistics[key]["range"]

        return {agent_name: state}

    def get_env_actions(self, action: dict) -> list:
        return [Action(ahu_idx=0, sps=act[0], das=act[1]) for act in action.values()]
        # return [Action(ahu_idx=0, sps=self._sps[self.env.tick], das=self._das[self.env.tick])]

    def get_reward(self, actions: list, tick: int):
        def get_attribute(name: str, idx: int=0, t: int=tick+1):
            return self.env.snapshot_list["ahus"][t:idx:name]

        diff_sps = abs(get_attribute("sps") - get_attribute("sps", t=tick))
        diff_das = abs(get_attribute("das") - get_attribute("das", t=tick))

        efficiency_ratio = abs(get_attribute("kw") / (get_attribute("at") + 1e-8))

        reward = -5

        if reward_config["type"] == "Bonsai":
            # Align with Bonsai
            reward = -5
            if diff_sps <= 0.3:
                reward = (
                    math.exp(-efficiency_ratio)
                    - 0.2 * diff_das
                    - (
                        max(0, 0.05 * (get_attribute("mat") - 68))   # mat is better to <= 68
                        * max(0, get_attribute("dat") - 57) # dat is better to <= 57
                    )
                )

        elif reward_config["type"] == "V2":
            reward = (
                reward_config["V2_efficiency_factor"] * math.exp(-efficiency_ratio)
                + reward_config["V2_das_diff_factor"] * diff_das
                + reward_config["V2_sps_diff_factor"] * diff_sps
                + (
                    max(0, reward_config["V2_constraints_factor"] * (get_attribute("mat") - 68))   # mat is better to <= 68
                    * max(0, get_attribute("dat") - 57) # dat is better to <= 57
                )
            )

            if reward_config["V2_lower_bound"] is not None:
                reward = max(reward, reward_config["V2_lower_bound"])

        elif reward_config["type"] == "V3":
            # The one Lei used
            if diff_sps <= 0.3:
                reward = (
                    2 * (self._statistics["kw"]["max"] - get_attribute("kw")) / self._statistics["kw"]["range"]
                    + (get_attribute("at") - self._statistics["at"]["min"]) / self._statistics["at"]["range"]
                    - 0.05 * diff_das
                    + 10 * (
                        max(0, 0.05 * (get_attribute("mat") - self._statistics["mat"]["mean"]))
                        * min(0, self._statistics["mat"]["mean"] - 11 - get_attribute("dat"))
                    )
                )
            else:
                reward = reward_config["V3_threshold"]

        elif reward_config["type"] == "V4":
            reward = -5
            if diff_sps <= 0.3:
                kw_line = (self._statistics["kw"]["mean"] + self._statistics["kw"]["min"]) / 2
                kw_gap = (kw_line - get_attribute("kw")) / self._statistics["kw"]["range"]

                diff_das /= self._statistics["das"]["range"]

                reward = 10 * (
                    math.exp(-efficiency_ratio)
                    + reward_config["V4_kw_factor"] * kw_gap * (2 if kw_gap > 0 else 1)
                    + reward_config["V4_das_diff_penalty_factor"] * diff_das * (1 if diff_das > 0.1 else 0)
                    + reward_config["V4_dat_penalty_factor"] * max(0, get_attribute("dat") - 57)
                )

                if self._logger:
                    self._logger.debug(
                        f"{math.exp(-efficiency_ratio)},"
                        f"{(kw_line - get_attribute('kw')[0]) / self._statistics['kw']['range']},"
                        f"{diff_das[0]},"
                        f"{get_attribute('dat')[0] - 57},"
                        f"{reward[0]/10}"
                    )

        elif reward_config["type"] == "V5":
            reward = -20
            if diff_sps <= 0.3 and get_attribute("das") < 60:
                kw_line = 70
                kw_gap = (kw_line - get_attribute("kw")) / self._statistics["kw"]["range"]

                reward = 10 * (
                    math.exp(-efficiency_ratio)
                    + reward_config["V5_kw_factor"] * kw_gap * (2 if kw_gap > 0 else 1)
                    + reward_config["V5_dat_penalty_factor"] * max(0, get_attribute("dat") - 57)
                )

                if self._logger:
                    self._logger.debug(
                        f"{math.exp(-efficiency_ratio)},"
                        f"{(kw_line - get_attribute('kw')[0]) / self._statistics['kw']['range']},"
                        f","
                        f"{get_attribute('dat')[0] - 57},"
                        f"{reward[0]/10}"
                    )

        return {agent_name: reward}

    def post_step(self, state, action, env_actions, reward, tick):
        if "reward" not in self.tracker:
            self.tracker["reward"] = []
        self.tracker["reward"].append(reward[agent_name][0] if isinstance(reward[agent_name], np.ndarray) else reward[agent_name])
        if tick == self.env._start_tick + self.env._durations - 1:
            for attribute in ["kw", "at", "dat", "mat"] + ["sps", "das"]:
                self.tracker[attribute] = self.env.snapshot_list["ahus"][::attribute][1:]
            self.tracker["total_kw"] = np.cumsum(self.tracker["kw"])
            self.tracker["reward"] = self.tracker["reward"][1:]
            self.tracker["total_reward"] = np.cumsum(self.tracker["reward"])


def get_env_sampler(logger: Logger):
    return HVACEnvSampler(
        get_env=lambda: Env(scenario="hvac", **env_config),
        get_policy_func_dict=policy_func_dict,
        agent2policy={agent_name: algorithm},
        get_test_env=lambda: Env(scenario="hvac", **env_config),
        logger=logger
    )