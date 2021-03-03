from pathlib import Path
from typing import Dict, List, Optional
from ruamel.yaml import YAML

from hunter.analysis import PerformanceLog
from hunter.config import Config
from hunter.data_selector import DataSelector
from hunter.importer import get_importer, Importer, CsvOptions
from hunter.test_config import TestConfig, TestType


class PerformanceTest:
    _test_conf: TestConfig
    importer: Importer
    _performance_log: Optional[PerformanceLog]

    def __init__(self, test_conf: TestConfig, conf: Config, csv_options: CsvOptions):
        self._test_conf = test_conf
        self.importer = get_importer(self._test_conf, conf, csv_options)
        # the case of Fallout-sourced tests, if no suffixes are specified in conf file, use all available ones
        if self._test_conf.type == TestType.Fallout and self._test_conf.suffixes is None:
            self._test_conf.suffixes = self.importer.fetch_all_suffixes(self._test_conf)
        self._performance_log = None

    def fetch(self, data_selector: DataSelector) -> PerformanceLog:
        self.performance_log = self.importer.fetch(self._test_conf, data_selector)
        return self.performance_log

    def find_change_points(self) -> PerformanceLog:
        assert self.performance_log is not None, "Performance log has not been initialized; try fetch() first"
        self.performance_log.find_change_points()
        return self.performance_log


class PerformanceTestGroup:
    _test_group_file: Path
    performance_tests: Dict[str, PerformanceTest]
    _performance_logs: Dict[str, Optional[PerformanceLog]]

    def __init__(self, test_group_file: Path, conf: Config, user: Optional[str], csv_options: CsvOptions):
        self._test_group_file = test_group_file
        yaml_content = self._load_yaml()
        self.performance_tests = {}
        self._performance_logs = {}
        for test_info_dict in yaml_content:
            test_name = test_info_dict["name"]
            user = test_info_dict.get("user", user)
            suffixes = test_info_dict.get("suffixes")
            test_conf = TestConfig(test_name, user, suffixes)
            self.performance_tests[test_name] = PerformanceTest(test_conf, conf, csv_options)
            self._performance_logs[test_name] = None

    def _load_yaml(self) -> List[Dict[str,str]]:
        try:
            content = self._test_group_file.read_text()
            yaml = YAML(typ='safe')
            return yaml.load(content)["tests"]
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Test group file not found: {e.filename}")
        except KeyError as e:
            raise KeyError(f"Test group file key not found: {e.args[0]}")

    def fetch(self, data_selector: DataSelector) -> Dict[str, PerformanceLog]:
        for test_name, perf_test in self.performance_tests.items():
            self._performance_logs[test_name] = perf_test.fetch(data_selector)
        return self._performance_logs

    def find_change_points(self) -> Dict[str, PerformanceLog]:
        for test_name, perf_test in self.performance_tests.items():
            self._performance_logs[test_name] = perf_test.find_change_points()
        return self._performance_logs
