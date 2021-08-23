# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from enum import Enum


class MsgTag(Enum):
    SAMPLE = "sample"
    TEST = "test"
    SAMPLE_DONE = "eval_done"
    TEST_DONE = "collect_done"
    INIT_POLICIES = "init_policies"
    INIT_POLICIES_DONE = "init_policies_done"
    POLICY_STATE = "policy_state"
    CHOOSE_ACTION = "choose_action"
    ACTION = "action"
    GET_INITIAL_POLICY_STATE = "get_initial_policy_state"
    LEARN = "learn"
    LEARN_DONE = "learn_finished"
    COMPUTE_GRAD = "compute_grad"
    COMPUTE_GRAD_DONE = "compute_grad_done"
    ABORT_ROLLOUT = "abort_rollout"
    DONE = "done"
    EXIT = "exit"
