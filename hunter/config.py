from dataclasses import dataclass
from pathlib import Path
from typing import List

from ruamel.yaml import YAML

from hunter.fallout import FalloutConfig
from hunter.graphite import GraphiteConfig


@dataclass
class Config:
    fallout: FalloutConfig
    graphite: GraphiteConfig


@dataclass
class ConfigError(Exception):
    message: str


def load_config_from(config_file: Path) -> Config:
    """Loads config from the specified location"""
    try:
        content = config_file.read_text()
        yaml = YAML(typ='safe')
        config = yaml.load(content)
        return Config(
            fallout=FalloutConfig(
                user=config["fallout"]["user"],
                token=config["fallout"]["token"],
                url=config["fallout"]["url"]),
            graphite=GraphiteConfig(
                url=config["graphite"]["url"],
                suffixes=config["graphite"]["suffixes"])
        )

    except FileNotFoundError as e:
        raise ConfigError(f"Configuration file not found: {e.filename}")
    except KeyError as e:
        raise ConfigError(f"Configuration key not found: {e.args[0]}")


def load_config() -> Config:
    """Loads config from the default location in ~/.hunter/conf.yaml"""
    return load_config_from(Path().home() / ".hunter/conf.yaml")
