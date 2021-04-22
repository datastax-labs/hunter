import argparse
import logging
from dataclasses import dataclass
from typing import Dict, Optional, List

from hunter import config
from hunter.attributes import get_back_links
from hunter.config import ConfigError, Config
from hunter.data_selector import DataSelector

from hunter.grafana import GrafanaError, Grafana, Annotation
from hunter.graphite import GraphiteError
from hunter.importer import DataImportError, Importers
from hunter.report import Report
from hunter.series import AnalysisOptions, ChangePointGroup
from hunter.slack import SlackNotifier, NotificationError
from hunter.test_config import TestConfigError, TestConfig, GraphiteTestConfig
from hunter.util import parse_datetime, DateFormatError


@dataclass
class HunterError(Exception):
    message: str


class Hunter:
    __conf: Config
    __importers: Importers
    __grafana: Optional[Grafana]
    __slack: Optional[SlackNotifier]

    def __init__(self, conf: Config):
        self.__conf = conf
        self.__importers = Importers(conf)
        self.__grafana = None
        self.__slack = self.__maybe_create_slack_notifier()

    def list_tests(self, group_names: Optional[List[str]]):
        if group_names is not None:
            test_names = []
            for group_name in group_names:
                group = self.__conf.test_groups.get(group_name)
                if group is None:
                    raise HunterError(f"Test group not found: {group_name}")
                test_names += (t.name for t in group)
        else:
            test_names = self.__conf.tests

        for test_name in sorted(test_names):
            print(test_name)

    def list_test_groups(self):
        for group_name in sorted(self.__conf.test_groups):
            print(group_name)

    def get_test(self, test_name: str) -> TestConfig:
        test = self.__conf.tests.get(test_name)
        if test is None:
            raise HunterError(f"Test not found {test_name}")
        return test

    def get_tests(self, *names: str) -> List[TestConfig]:
        tests = []
        for name in names:
            group = self.__conf.test_groups.get(name)
            if group is not None:
                tests += group
            else:
                test = self.__conf.tests.get(name)
                if test is not None:
                    tests.append(test)
                else:
                    raise HunterError(f"Test or group not found: {name}")
        return tests

    def list_metrics(self, test: TestConfig):
        importer = self.__importers.get(test)
        for metric_name in importer.fetch_all_metric_names(test):
            print(metric_name)

    def analyze(
        self, test: TestConfig, selector: DataSelector, options: AnalysisOptions
    ) -> List[ChangePointGroup]:
        importer = self.__importers.get(test)
        series = importer.fetch_data(test, selector)
        change_points = series.analyze(options).change_points_by_time
        report = Report(series, change_points)
        print(test.name + ":")
        print(report.format_log_annotated())
        return change_points

    def __get_grafana(self) -> Grafana:
        if self.__grafana is None:
            self.__grafana = Grafana(self.__conf.grafana)
        return self.__grafana

    def update_grafana(self, test: GraphiteTestConfig, change_points: List[ChangePointGroup]):
        logging.info(f"Determining new Grafana annotations for test {test.name}...")
        grafana = self.__get_grafana()
        annotations = []
        for change_point in change_points:
            annotation_text = get_back_links(change_point.attributes)
            for change in change_point.changes:
                path = test.get_path(change.metric)
                matching_dashboard_panels = grafana.find_all_dashboard_panels_displaying(path)
                for dashboard_panel in matching_dashboard_panels:
                    # Grafana timestamps have 13 digits, Graphite timestamps have 10
                    # (hence multiplication by 10^3)
                    annotations.append(
                        Annotation(
                            dashboard_id=dashboard_panel["dashboard id"],
                            panel_id=dashboard_panel["panel id"],
                            time=change_point.time * 10 ** 3,
                            text=annotation_text,
                            tags=dashboard_panel["tags"],
                        )
                    )
        if len(annotations) == 0:
            logging.info("No Grafana panels to update")
        else:
            logging.info("Updating Grafana with latest annotations...")
            # sorting annotations in this order makes logging output easier to look through
            annotations.sort(key=lambda a: (a.dashboard_id, a.panel_id, a.time))
            for annotation in annotations:
                # remove existing annotations with same dashboard id, panel id, and tags
                grafana.delete_matching_annotations(annotation=annotation)
            for annotation in annotations:
                grafana.post_annotation(annotation)

    def __maybe_create_slack_notifier(self):
        if not self.__conf.slack:
            return None
        return SlackNotifier(self.__conf.slack)

    def notify_slack(
        self, test_change_points: Dict[str, List[ChangePointGroup]], selector: DataSelector
    ):
        if not self.__slack:
            logging.error(
                "Slack definition is missing from the configuration, cannot send notification"
            )
            return
        self.__slack.notify(test_change_points, selector=selector)


def setup_data_selector_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--metrics",
        metavar="LIST",
        dest="metrics",
        help="a comma-separated list of metrics to analyze",
    )
    parser.add_argument(
        "--attrs",
        metavar="LIST",
        dest="attributes",
        help="a comma-separated list of attribute names associated with the runs "
        "(e.g. commit, branch, version); "
        "if not specified, it will be automatically filled based on available information",
    )
    since_group = parser.add_mutually_exclusive_group()
    since_group.add_argument(
        "--since-commit",
        metavar="STRING",
        dest="since_commit",
        help="The commit at the start of the time span to analyze",
    )
    since_group.add_argument(
        "--since-version",
        metavar="STRING",
        dest="since_version",
        help="The version at the start of the time span to analyze",
    )
    since_group.add_argument(
        "--since",
        metavar="DATE",
        dest="since_time",
        help="the start of the time span to analyze; "
        "accepts ISO, and human-readable dates like '10 weeks ago'",
    )
    until_group = parser.add_mutually_exclusive_group()
    until_group.add_argument(
        "--until-commit",
        metavar="STRING",
        dest="until_commit",
        help="The commit at the end of the time span to analyze",
    )
    until_group.add_argument(
        "--until-version",
        metavar="STRING",
        dest="until_version",
        help="The version at the end of the time span to analyze",
    )
    until_group.add_argument(
        "--until",
        metavar="DATE",
        dest="until_time",
        help="the end of the time span to analyze; same syntax as --since",
    )


def data_selector_from_args(args: argparse.Namespace) -> DataSelector:
    data_selector = DataSelector()
    if args.metrics is not None:
        data_selector.metrics = list(args.metrics.split(","))
    if args.attributes is not None:
        data_selector.attributes = list(args.attributes.split(","))
    if args.since_commit is not None:
        data_selector.since_commit = args.since_commit
    if args.since_version is not None:
        data_selector.since_version = args.since_version
    if args.since_time is not None:
        data_selector.since_time = parse_datetime(args.since_time)
    if args.until_commit is not None:
        data_selector.until_commit = args.until_commit
    if args.until_version is not None:
        data_selector.until_version = args.until_version
    if args.until_time is not None:
        data_selector.until_time = parse_datetime(args.until_time)
    return data_selector


def setup_analysis_options_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-P, --p-value",
        dest="pvalue",
        type=float,
        default=0.001,
        help="maximum accepted P-value of a change-point; "
        "P denotes the probability that the change-point has "
        "been found by a random coincidence, rather than a real "
        "difference between the data distributions",
    )
    parser.add_argument(
        "-M",
        "--magnitude",
        dest="magnitude",
        type=float,
        default=0.0,
        help="minimum accepted magnitude of a change-point "
        "computed as abs(new_mean / old_mean - 1.0); use it "
        "to filter out stupidly small changes like < 0.01",
    )
    parser.add_argument(
        "--window",
        default=50,
        type=int,
        dest="window",
        help="the number of data points analyzed at once; "
        "the window size affects the discriminative "
        "power of the change point detection algorithm; "
        "large windows are less susceptible to noise; "
        "however, a very large window may cause dismissing short regressions "
        "as noise so it is best to keep it short enough to include not more "
        "than a few change points (optimally at most 1)",
    )


def analysis_options_from_args(args: argparse.Namespace) -> AnalysisOptions:
    conf = AnalysisOptions()
    if args.pvalue is not None:
        conf.max_pvalue = args.pvalue
    if args.magnitude is not None:
        conf.min_magnitude = args.magnitude
    if args.window is not None:
        conf.window_len = args.window
    return conf


def main():
    logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Hunts performance regressions in Fallout results")

    subparsers = parser.add_subparsers(dest="command")
    list_tests_parser = subparsers.add_parser("list-tests", help="list available tests")
    list_tests_parser.add_argument("group", help="name of the group of the tests", nargs="*")

    list_metrics_parser = subparsers.add_parser(
        "list-metrics", help="list available metrics for a test"
    )
    list_metrics_parser.add_argument("test", help="name of the test")

    subparsers.add_parser("list-groups", help="list available groups of tests")

    analyze_parser = subparsers.add_parser(
        "analyze",
        help="analyze performance test results",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    analyze_parser.add_argument("tests", help="name of the test or group of the tests", nargs="+")
    analyze_parser.add_argument(
        "--update-grafana",
        help="Update Grafana dashboards with appropriate annotations of change points",
        action="store_true",
    )
    analyze_parser.add_argument(
        "--notify-slack",
        help="Send notification to Slack channel declared in configuration containing a summary of change points",
        action="store_true",
    )

    setup_data_selector_parser(analyze_parser)
    setup_analysis_options_parser(analyze_parser)

    try:
        args = parser.parse_args()
        conf = config.load_config()
        hunter = Hunter(conf)

        if args.command == "list-groups":
            hunter.list_test_groups()

        if args.command == "list-tests":
            group_names = args.group if args.group else None
            hunter.list_tests(group_names)

        if args.command == "list-metrics":
            test = hunter.get_test(args.test)
            hunter.list_metrics(test)

        if args.command == "analyze":
            update_grafana_flag = args.update_grafana
            notify_slack_flag = args.notify_slack
            data_selector = data_selector_from_args(args)
            options = analysis_options_from_args(args)
            tests = hunter.get_tests(*args.tests)
            tests_change_points = dict()
            for test in tests:
                try:
                    change_points = hunter.analyze(test, selector=data_selector, options=options)
                    if update_grafana_flag:
                        if not isinstance(test, GraphiteTestConfig):
                            raise GrafanaError(f"Not a Graphite test")
                        hunter.update_grafana(test, change_points)
                    if notify_slack_flag and change_points:
                        tests_change_points[test.name] = change_points
                except DataImportError as err:
                    logging.error(err.message)
                except GrafanaError as err:
                    logging.error(
                        f"Failed to update grafana dashboards for {test.name}: {err.message}"
                    )
            if notify_slack_flag:
                hunter.notify_slack(tests_change_points, selector=data_selector)

        if args.command is None:
            parser.print_usage()

    except ConfigError as err:
        logging.error(err.message)
        exit(1)
    except TestConfigError as err:
        logging.error(err.message)
        exit(1)
    except GraphiteError as err:
        logging.error(err.message)
        exit(1)
    except GrafanaError as err:
        logging.error(err.message)
        exit(1)
    except DataImportError as err:
        logging.error(err.message)
        exit(1)
    except HunterError as err:
        logging.error(err.message)
        exit(1)
    except DateFormatError as err:
        logging.error(err.message)
        exit(1)
    except NotificationError as err:
        logging.error(err.message)
        exit(1)


if __name__ == "__main__":
    main()
