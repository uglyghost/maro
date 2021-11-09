from abc import abstractmethod
from typing import Callable, Dict, List

from maro.rl_v3.policy_trainer import AbsTrainer


class AbsTrainerManager(object):
    def __init__(self) -> None:
        super(AbsTrainerManager, self).__init__()

    @abstractmethod
    def train(self) -> None:
        pass

    @abstractmethod
    def get_policy_states(self) -> Dict[str, Dict[str, object]]:
        pass


class SimpleTrainerManager(AbsTrainerManager):
    def __init__(
        self,
        get_trainer_func_dict: Dict[str, Callable[[str], AbsTrainer]]
    ) -> None:
        super(SimpleTrainerManager, self).__init__()
        self._trainers = [func(name) for name, func in get_trainer_func_dict.items()]

    def train(self) -> None:
        for trainer in self._trainers:
            trainer.train_step()

    def get_policy_states(self) -> Dict[str, Dict[str, object]]:
        return {trainer.name: trainer.get_policy_state_dict() for trainer in self._trainers}