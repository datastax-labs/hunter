from dataclasses import dataclass
from pathlib import Path
from ruamel.yaml import YAML

from hunter.fallout import FalloutConfig
from hunter.grafana import GrafanaConfig
from hunter.graphite import GraphiteConfig


@dataclass
class Config:
    fallout: FalloutConfig
    graphite: GraphiteConfig
    grafana: GrafanaConfig


@dataclass
class ConfigError(Exception):
    message: str


def load_config_from(config_file: Path) -> Config:
    """Loads config from the specified location"""
    try:
        content = config_file.read_text()
        yaml = YAML(typ="safe")
        config = yaml.load(content)
        # if Grafana configs not explicitly set in yaml file, default to same as Graphite server at port 3000
        if config.get("grafana") is None:
            config["gafana"] = {}
            config["grafana"]["url"] = f"{config['graphite']['url'].strip('/')}:3000/"
            config["grafana"]["user"] = "admin"
            config["grafana"]["password"] = "admin"
        return Config(
            fallout=FalloutConfig(
                user=config["fallout"]["user"],
                token=config["fallout"]["token"],
                url=config["fallout"]["url"],
            ),
            graphite=GraphiteConfig(
                url=config["graphite"]["url"], suffixes=config["graphite"].get("suffixes")
            ),
            grafana=GrafanaConfig(
                url=config["grafana"]["url"],
                user=config["grafana"]["user"],
                password=config["grafana"]["password"],
            ),
        )

    except FileNotFoundError as e:
        raise ConfigError(f"Configuration file not found: {e.filename}")
    except KeyError as e:
        raise ConfigError(f"Configuration key not found: {e.args[0]}")


def load_config() -> Config:
    """Loads config from the default location in ~/.hunter/conf.yaml"""
    return load_config_from(Path().home() / ".hunter/conf.yaml")
