import os
from dataclasses import dataclass
from expandvars import expandvars
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
        content = expandvars(config_file.read_text())
        yaml = YAML(typ="safe")
        config = yaml.load(content)
        # if Grafana configs not explicitly set in yaml file, default to same as Graphite server at port 3000
        if config.get("grafana") is None:
            config["gafana"] = {}
            config["grafana"]["url"] = f"{config['graphite']['url'].strip('/')}:3000/"
            config["grafana"]["user"] = os.environ.get("GRAFANA_USER", "admin")
            config["grafana"]["password"] = os.environ.get("GRAFANA_PASSWORD", "admin")

        fallout_user = config["fallout"].get("user")
        if fallout_user is None:
            fallout_user = os.environ.get("FALLOUT_USER", "")
        fallout_token = config["fallout"].get("token")
        if fallout_token is None:
            fallout_token = os.environ.get("FALLOUT_OAUTH_TOKEN", "")

        return Config(
            fallout=FalloutConfig(
                url=config["fallout"]["url"],
                user=fallout_user,
                token=fallout_token,
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
    """Loads config from one of the default locations"""
    paths = [Path().home() / ".hunter/hunter.yaml",
             Path().home() / ".hunter/conf.yaml",
             Path(os.path.realpath(__file__)) / "resources/hunter.yaml"]

    for p in paths:
        if p.exists():
            return load_config_from(p)

    raise ConfigError(f"No configuration file found. Searched: {paths}")

