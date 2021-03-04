from dataclasses import dataclass
from pathlib import Path
from ruamel.yaml import YAML
from typing import Dict, Optional, List

from hunter.csv import CsvOptions


@dataclass
class TestConfig:
    name: str


@dataclass
class TestConfigError(Exception):
    message: str


@dataclass
class CsvTestConfig(TestConfig):
    csv_options: CsvOptions

    def __init__(self, name: str, csv_options: Optional[CsvOptions] = CsvOptions()):
        super().__init__(name=name)
        self.csv_options = csv_options


@dataclass
class FalloutTestConfig(TestConfig):
    user: str
    suffixes: Optional[List[str]]

    def __init__(self, name: str, user: Optional[str] = None, suffixes: Optional[List[str]] = None):
        super().__init__(name=name)
        self.user = user
        self.suffixes = suffixes


def create_test_config(test_info: Dict, csv_options: CsvOptions, user: Optional[str] = None) -> TestConfig:
    try:
        test_name = test_info["name"]
        if test_name.endswith('csv'):
            return create_csv_test_config(test_info, csv_options)
        else:
            return create_fallout_test_config(test_info, user)
    except KeyError as e:
        raise TestConfigError(f"Test configuration key not found: {e.args[0]}")


def create_csv_test_config(test_info: Dict, csv_options: CsvOptions) -> CsvTestConfig:
    test_name = test_info['name']
    if test_info.get('csv_options'):
        csv_options.delimiter = test_info['csv_options'].get(
            'delimiter',
            csv_options.delimiter
        )
        csv_options.quote_char = test_info['csv_options'].get(
            'quote_char',
            csv_options.quote_char
        )
        csv_options.time_column = test_info['csv_options'].get(
            'time_column',
            csv_options.time_column
        )
    return CsvTestConfig(name=test_name, csv_options=csv_options)


def create_fallout_test_config(test_info: Dict, user: Optional[str] = None) -> FalloutTestConfig:
    test_name = test_info['name']
    user = test_info.get('user', user)
    suffixes = test_info.get('suffixes')
    return FalloutTestConfig(name=test_name, user=user, suffixes=suffixes)


@dataclass
class TestGroup:
    """
    Effectively serves as a container for a bunch of TestConfig objects. Note that the contained TestConfig
    objects can be a mixed of CsvTestConfig and FalloutTestConfig objects.
    """
    test_group_file: Path
    test_configs: Dict[str, TestConfig]

    def __init__(self, test_group_file: Path, user: Optional[str], csv_options: CsvOptions):
        self.test_group_file = test_group_file
        yaml_content = self._load_yaml()
        self.test_configs = {}
        for test_info in yaml_content:
            try:
                test_name = test_info['name']
                self.test_configs[test_name] = create_test_config(test_info, csv_options, user)
            except KeyError as e:
                raise TestConfigError(f"Test configuration key not found: {e.args[0]}")

    def _load_yaml(self) -> List[Dict[str,str]]:
        try:
            content = self.test_group_file.read_text()
            yaml = YAML(typ='safe')
            return yaml.load(content)["tests"]
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Test group file not found: {e.filename}")
        except KeyError as e:
            raise KeyError(f"Test group file key not found: {e.args[0]}")
