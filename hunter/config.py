import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from expandvars import expandvars
from ruamel.yaml import YAML

from hunter.bigquery import BigQueryConfig
from hunter.grafana import GrafanaConfig
from hunter.graphite import GraphiteConfig
from hunter.postgres import PostgresConfig
from hunter.slack import SlackConfig
from hunter.test_config import TestConfig, create_test_config
from hunter.util import merge_dict_list


@dataclass
class Config:
    graphite: Optional[GraphiteConfig]
    grafana: Optional[GrafanaConfig]
    tests: Dict[str, TestConfig]
    test_groups: Dict[str, List[TestConfig]]
    slack: SlackConfig
    postgres: PostgresConfig
    bigquery: BigQueryConfig


@dataclass
class ConfigError(Exception):
    message: str


def load_templates(config: Dict) -> Dict[str, Dict]:
    templates = config.get("templates", {})
    if not isinstance(templates, Dict):
        raise ConfigError("Property `templates` is not a dictionary")
    return templates


def load_tests(config: Dict, templates: Dict) -> Dict[str, TestConfig]:
    tests = config.get("tests", {})
    if not isinstance(tests, Dict):
        raise ConfigError("Property `tests` is not a dictionary")

    result = {}
    for (test_name, test_config) in tests.items():
        template_names = test_config.get("inherit", [])
        if not isinstance(template_names, List):
            template_names = [templates]
        try:
            template_list = [templates[name] for name in template_names]
        except KeyError as e:
            raise ConfigError(f"Template {e.args[0]} referenced in test {test_name} not found")
        test_config = merge_dict_list(template_list + [test_config])
        result[test_name] = create_test_config(test_name, test_config)

    return result


def load_test_groups(config: Dict, tests: Dict[str, TestConfig]) -> Dict[str, List[TestConfig]]:
    groups = config.get("test_groups", {})
    if not isinstance(groups, Dict):
        raise ConfigError("Property `test_groups` is not a dictionary")

    result = {}
    for (group_name, test_names) in groups.items():
        test_list = []
        if not isinstance(test_names, List):
            raise ConfigError(f"Test group {group_name} must be a list")
        for test_name in test_names:
            test_config = tests.get(test_name)
            if test_config is None:
                raise ConfigError(f"Test {test_name} referenced by group {group_name} not found.")
            test_list.append(test_config)

        result[group_name] = test_list

    return result


def load_config_from(config_file: Path) -> Config:
    """Loads config from the specified location"""
    try:
        content = expandvars(config_file.read_text(), nounset=True)
        yaml = YAML(typ="safe")
        config = yaml.load(content)
        """
        if Grafana configs not explicitly set in yaml file, default to same as Graphite
        server at port 3000
        """
        graphite_config = None
        grafana_config = None
        if "graphite" in config:
            if "url" not in config["graphite"]:
                raise ValueError("graphite.url")
            graphite_config = GraphiteConfig(url=config["graphite"]["url"])
            if config.get("grafana") is None:
                config["grafana"] = {}
                config["grafana"]["url"] = f"{config['graphite']['url'].strip('/')}:3000/"
                config["grafana"]["user"] = os.environ.get("GRAFANA_USER", "admin")
                config["grafana"]["password"] = os.environ.get("GRAFANA_PASSWORD", "admin")
            grafana_config = GrafanaConfig(
                url=config["grafana"]["url"],
                user=config["grafana"]["user"],
                password=config["grafana"]["password"],
            )

        slack_config = None
        if config.get("slack") is not None:
            if not config["slack"]["token"]:
                raise ValueError("slack.token")
            slack_config = SlackConfig(
                bot_token=config["slack"]["token"],
            )

        postgres_config = None
        if config.get("postgres") is not None:
            if not config["postgres"]["hostname"]:
                raise ValueError("postgres.hostname")
            if not config["postgres"]["port"]:
                raise ValueError("postgres.port")
            if not config["postgres"]["username"]:
                raise ValueError("postgres.username")
            if not config["postgres"]["password"]:
                raise ValueError("postgres.password")
            if not config["postgres"]["database"]:
                raise ValueError("postgres.database")

            postgres_config = PostgresConfig(
                hostname=config["postgres"]["hostname"],
                port=config["postgres"]["port"],
                username=config["postgres"]["username"],
                password=config["postgres"]["password"],
                database=config["postgres"]["database"],
            )

        bigquery_config = None
        if config.get("bigquery") is not None:
            bigquery_config = BigQueryConfig(
                project_id=config["bigquery"]["project_id"],
                dataset=config["bigquery"]["dataset"],
                credentials=config["bigquery"]["credentials"],
            )

        templates = load_templates(config)
        tests = load_tests(config, templates)
        groups = load_test_groups(config, tests)

        return Config(
            graphite=graphite_config,
            grafana=grafana_config,
            slack=slack_config,
            postgres=postgres_config,
            bigquery=bigquery_config,
            tests=tests,
            test_groups=groups,
        )

    except FileNotFoundError as e:
        raise ConfigError(f"Configuration file not found: {e.filename}")
    except KeyError as e:
        raise ConfigError(f"Configuration key not found: {e.args[0]}")
    except ValueError as e:
        raise ConfigError(f"Value for configuration key not found: {e.args[0]}")


def load_config() -> Config:
    """Loads config from one of the default locations"""

    env_config_path = os.environ.get("HUNTER_CONFIG")
    if env_config_path:
        return load_config_from(Path(env_config_path).absolute())

    paths = [
        Path().home() / ".hunter/hunter.yaml",
        Path().home() / ".hunter/conf.yaml",
        Path(os.path.realpath(__file__)).parent / "resources/hunter.yaml",
    ]

    for p in paths:
        if p.exists():
            return load_config_from(p)

    raise ConfigError(f"No configuration file found. Checked $HUNTER_CONFIG and searched: {paths}")
