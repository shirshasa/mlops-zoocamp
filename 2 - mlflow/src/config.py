import os
from dataclasses import dataclass
import yaml


DEFAULT_CONFIG_FILEPATH = '../configs/config.yaml'


@dataclass
class Configs:
    """
    Configs class.
    This file centralizes anything that can be parametrized in the code.
    """
    def __init__(self, configs) -> None:
        for key, value in configs.items():
            setattr(self, key, value)

    @classmethod
    def from_file(cls, config_file_path=DEFAULT_CONFIG_FILEPATH):
        """
        Get configs from yaml file.
        """
        configs = {}
        config_file_path = os.path.join(os.path.dirname(__file__), config_file_path)
        with open(config_file_path, encoding='utf-8') as config_file:
            configs = yaml.load(config_file, Loader=yaml.FullLoader)
        return cls(configs)