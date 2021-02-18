from pathlib import Path
from typing import Dict, List, Optional
from ruamel.yaml import YAML

from hunter.analysis import PerformanceLog
from hunter.data_selector import DataSelector
from hunter.importer import Importer, CsvImporter, FalloutImporter
from hunter.report import Report


class PerformanceTest:
    importer: Importer
    data_selector: DataSelector
    performance_log: Optional[PerformanceLog]
    report = Optional[Report]

    def __init__(self, importer: Importer, data_selector: DataSelector):
        self.importer = importer
        self.data_selector = data_selector
        self.performance_log = None
        self.report = None

    def fetch(self) -> PerformanceLog:
        raise NotImplementedError

    def find_change_points(self) -> PerformanceLog:
        if self.performance_log is None:
            self.fetch()
        self.performance_log.find_change_points()
        return self.performance_log

    def get_report(self) -> Report:
        if self.performance_log is None:
            self.find_change_points()
        if self.report is None:
            self.report = Report(self.performance_log)
        return self.report


class CsvPerformanceTest(PerformanceTest):
    file: Path

    def __init__(self, file: Path, importer: CsvImporter, data_selector: DataSelector):
        super().__init__(importer=importer, data_selector=data_selector)
        self.file = file

    def fetch(self) -> PerformanceLog:
        if self.performance_log is None:
            self.performance_log = self.importer.fetch(file=self.file, selector=self.data_selector)
        return self.performance_log


class FalloutPerformanceTest(PerformanceTest):
    test_name: str
    user: Optional[str]

    def __init__(self, test_name: str, user: Optional[str], importer: FalloutImporter, data_selector: DataSelector):
        super().__init__(importer=importer, data_selector=data_selector)
        self.test_name = test_name
        self.user = user

    def fetch(self) -> PerformanceLog:
        if self.performance_log is None:
            self.performance_log = self.importer.fetch(self.test_name, self.user, self.data_selector)
        return self.performance_log


class FalloutPerformanceTestGroup:
    """
    Effectively serves as a list of FalloutPerformanceTest objects, with appropriate API
    """
    test_group_file: Path
    test_names: List[str]
    performance_tests: Dict[str, FalloutPerformanceTest]

    def __init__(
            self,
            test_group_file: Path,
            user: Optional[str],
            importer: FalloutImporter,
            template_data_selector: DataSelector):
        try:
            self.test_group_file = test_group_file
            content = self.test_group_file.read_text()
            yaml = YAML(typ='safe')
            test_group_dict = yaml.load(content)
            self.test_names = []
            self.performance_tests = {}
            for test in test_group_dict.get('tests'):
                test_name = test['name']
                suffixes =  test.get('suffixes')
                if suffixes is None:
                    suffixes = importer.fetch_suffixes(test_name, user)
                data_selector = DataSelector()
                data_selector.suffixes = suffixes
                data_selector.from_time = template_data_selector.from_time
                data_selector.until_time = template_data_selector.until_time
                data_selector.attributes = template_data_selector.attributes
                self.test_names.append(test_name)
                self.performance_tests[test_name] = FalloutPerformanceTest(test_name, user, importer, data_selector)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Test group file not found: {e.filename}")
        except KeyError as e:
            raise KeyError(f"Test group file key not found: {e.args[0]}")

    def fetch(self) -> Dict[str, PerformanceLog]:
        performance_log_dict = {}
        for test_name, perf_test in self.performance_tests.items():
            performance_log_dict[test_name] = perf_test.fetch()
        return performance_log_dict

    def find_change_points(self) -> Dict[str, PerformanceLog]:
        performance_log_dict = {}
        for test_name, perf_test in self.performance_tests.items():
            performance_log_dict[test_name] = perf_test.find_change_points()
        return performance_log_dict

    def get_report(self, test_name: Optional[str]) -> Report:
        assert test_name in self.test_names, f"{test_name} not part of this test group. " \
                                             f"Available test names: {self.test_names}"
        if self.performance_tests[test_name].report is None:
            return self.performance_tests[test_name].get_report()

    def get_reports(self) -> Dict[str, Report]:
        report_dict = {}
        for test_name, perf_test in self.performance_tests.items():
            report_dict[test_name] = self.get_report(test_name)
        return report_dict
