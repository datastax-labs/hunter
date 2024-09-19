import argparse
import copy
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pytz
from slack_sdk import WebClient

from hunter import config
from hunter.attributes import get_back_links
from hunter.bigquery import BigQuery, BigQueryError
from hunter.config import Config, ConfigError
from hunter.data_selector import DataSelector
from hunter.grafana import Annotation, Grafana, GrafanaError
from hunter.graphite import GraphiteError
from hunter.importer import DataImportError, Importers
from hunter.postgres import Postgres, PostgresError
from hunter.report import ChangePointReport, RegressionsReport, ReportType
from hunter.series import AnalysisOptions, AnalyzedSeries, compare
from hunter.slack import NotificationError, SlackNotifier
from hunter.test_config import (
    BigQueryTestConfig,
    GraphiteTestConfig,
    PostgresTestConfig,
    TestConfig,
    TestConfigError,
)
from hunter.util import DateFormatError, interpolate, parse_datetime


@dataclass
class HunterError(Exception):
    message: str


class Hunter:
    __conf: Config
    __importers: Importers
    __grafana: Optional[Grafana]
    __slack: Optional[SlackNotifier]
    __postgres: Optional[Postgres]
    __bigquery: Optional[BigQuery]

    def __init__(self, conf: Config):
        self.__conf = conf
        self.__importers = Importers(conf)
        self.__grafana = None
        self.__slack = self.__maybe_create_slack_notifier()
        self.__postgres = None
        self.__bigquery = None

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
        self,
        test: TestConfig,
        selector: DataSelector,
        options: AnalysisOptions,
        report_type: ReportType,
    ) -> AnalyzedSeries:
        importer = self.__importers.get(test)
        series = importer.fetch_data(test, selector)
        analyzed_series = series.analyze(options)
        change_points = analyzed_series.change_points_by_time
        report = ChangePointReport(series, change_points)
        produced_report = report.produce_report(test.name, report_type)
        print(produced_report)
        return analyzed_series

    def __get_grafana(self) -> Grafana:
        if self.__grafana is None:
            self.__grafana = Grafana(self.__conf.grafana)
        return self.__grafana

    def update_grafana_annotations(self, test: GraphiteTestConfig, series: AnalyzedSeries):
        grafana = self.__get_grafana()
        begin = datetime.fromtimestamp(series.time()[0], tz=pytz.UTC)
        end = datetime.fromtimestamp(series.time()[len(series.time()) - 1], tz=pytz.UTC)

        logging.info(f"Fetching Grafana annotations for test {test.name}...")
        tags_to_query = ["hunter", "change-point", "test:" + test.name]
        old_annotations_for_test = grafana.fetch_annotations(begin, end, list(tags_to_query))
        logging.info(f"Found {len(old_annotations_for_test)} annotations")

        created_count = 0
        for metric_name, change_points in series.change_points.items():
            path = test.get_path(series.branch_name(), metric_name)
            metric_tag = f"metric:{metric_name}"
            tags_to_create = (
                tags_to_query
                + [metric_tag]
                + test.tags
                + test.annotate
                + test.metrics[metric_name].annotate
            )

            substitutions = {
                "TEST_NAME": test.name,
                "METRIC_NAME": metric_name,
                "GRAPHITE_PATH": [path],
                "GRAPHITE_PATH_COMPONENTS": path.split("."),
                "GRAPHITE_PREFIX": [test.prefix],
                "GRAPHITE_PREFIX_COMPONENTS": test.prefix.split("."),
            }

            tmp_tags_to_create = []
            for t in tags_to_create:
                tmp_tags_to_create += interpolate(t, substitutions)
            tags_to_create = tmp_tags_to_create

            old_annotations = [a for a in old_annotations_for_test if metric_tag in a.tags]
            old_annotation_times = set((a.time for a in old_annotations if a.tags))

            target_annotations = []
            for cp in change_points:
                attributes = series.attributes_at(cp.index)
                annotation_text = get_back_links(attributes)
                target_annotations.append(
                    Annotation(
                        id=None,
                        time=datetime.fromtimestamp(cp.time, tz=pytz.UTC),
                        text=annotation_text,
                        tags=tags_to_create,
                    )
                )
            target_annotation_times = set((a.time for a in target_annotations))

            to_delete = [a for a in old_annotations if a.time not in target_annotation_times]
            if to_delete:
                logging.info(
                    f"Removing {len(to_delete)} annotations "
                    f"for test {test.name} and metric {metric_name}..."
                )
                grafana.delete_annotations(*(a.id for a in to_delete))

            to_create = [a for a in target_annotations if a.time not in old_annotation_times]
            if to_create:
                logging.info(
                    f"Creating {len(to_create)} annotations "
                    f"for test {test.name} and metric {metric_name}..."
                )
                grafana.create_annotations(*to_create)
                created_count += len(to_create)

        if created_count == 0:
            logging.info("All annotations up-to-date. No new annotations needed.")
        else:
            logging.info(f"Created {created_count} annotations.")

    def remove_grafana_annotations(self, test: Optional[TestConfig], force: bool):
        """Removes all Hunter annotations (optionally for a given test) in Grafana"""
        grafana = self.__get_grafana()
        if test:
            logging.info(f"Fetching Grafana annotations for test {test.name}...")
        else:
            logging.info("Fetching Grafana annotations...")
        tags_to_query = {"hunter", "change-point"}
        if test:
            tags_to_query.add(f"test: {test.name}")
        annotations = grafana.fetch_annotations(None, None, list(tags_to_query))
        if not annotations:
            logging.info("No annotations found.")
            return
        if not force:
            print(
                f"Are you sure to remove {len(annotations)} annotations from {grafana.url}? [y/N]"
            )
            decision = input().strip()
            if decision.lower() != "y" and decision.lower() != "yes":
                return
        logging.info(f"Removing {len(annotations)} annotations...")
        grafana.delete_annotations(*(a.id for a in annotations))

    def __get_postgres(self) -> Postgres:
        if self.__postgres is None:
            self.__postgres = Postgres(self.__conf.postgres)
        return self.__postgres

    def __get_bigquery(self) -> BigQuery:
        if self.__bigquery is None:
            self.__bigquery = BigQuery(self.__conf.bigquery)
        return self.__bigquery

    def update_postgres(self, test: PostgresTestConfig, series: AnalyzedSeries):
        postgres = self.__get_postgres()
        for metric_name, change_points in series.change_points.items():
            for cp in change_points:
                attributes = series.attributes_at(cp.index)
                postgres.insert_change_point(test, metric_name, attributes, cp)

    def update_bigquery(self, test: BigQueryTestConfig, series: AnalyzedSeries):
        bigquery = self.__get_bigquery()
        for metric_name, change_points in series.change_points.items():
            for cp in change_points:
                attributes = series.attributes_at(cp.index)
                bigquery.insert_change_point(test, metric_name, attributes, cp)

    def regressions(
        self,
        test: TestConfig,
        selector: DataSelector,
        options: AnalysisOptions,
        report_type: ReportType,
        ignore_direction: bool = False,
    ) -> bool:
        importer = self.__importers.get(test)

        # Even if user is interested only in performance difference since some point X,
        # we really need to fetch some earlier points than X.
        # Otherwise, if performance went down very early after X, e.g. at X + 1, we'd have
        # insufficient number of data points to compute the baseline performance.
        # Instead of using `since-` selector, we're fetching everything from the
        # beginning and then we find the baseline performance around the time pointed by
        # the original selector.
        since_version = selector.since_version
        since_commit = selector.since_commit
        since_time = selector.since_time
        baseline_selector = copy.deepcopy(selector)
        baseline_selector.last_n_points = sys.maxsize
        baseline_selector.branch = None
        baseline_selector.since_version = None
        baseline_selector.since_commit = None
        baseline_selector.since_time = since_time - timedelta(days=30)
        baseline_series = importer.fetch_data(test, baseline_selector)

        if since_version:
            baseline_index = baseline_series.find_by_attribute("version", since_version)
            if not baseline_index:
                raise HunterError(f"No runs of test {test.name} with version {since_version}")
            baseline_index = max(baseline_index)
        elif since_commit:
            baseline_index = baseline_series.find_by_attribute("commit", since_commit)
            if not baseline_index:
                raise HunterError(f"No runs of test {test.name} with commit {since_commit}")
            baseline_index = max(baseline_index)
        else:
            baseline_index = baseline_series.find_first_not_earlier_than(since_time)

        baseline_series = baseline_series.analyze(options=options)

        if selector.branch:
            target_series = importer.fetch_data(test, selector).analyze(options=options)
        else:
            target_series = baseline_series

        cmp = compare(baseline_series, baseline_index, target_series, target_series.len())
        regressions = []
        for metric_name, stats in cmp.stats.items():
            direction = baseline_series.metric(metric_name).direction
            m1 = stats.mean_1
            m2 = stats.mean_2
            if ignore_direction:
                mean_diff = m2 != m1
            else:
                mean_diff = m2 * direction < m1 * direction

            if mean_diff and stats.pvalue < options.max_pvalue:
                regressions.append((metric_name, stats))

        report = RegressionsReport(regressions)
        produced_report = report.produce_report(test.name, report_type)
        print(produced_report)
        return len(regressions) > 0

    def __maybe_create_slack_notifier(self):
        if not self.__conf.slack:
            return None
        return SlackNotifier(WebClient(token=self.__conf.slack.bot_token))

    def notify_slack(
        self,
        test_change_points: Dict[str, AnalyzedSeries],
        selector: DataSelector,
        channels: List[str],
        since: datetime,
    ):
        if not self.__slack:
            logging.error(
                "Slack definition is missing from the configuration, cannot send notification"
            )
            return
        self.__slack.notify(test_change_points, selector=selector, channels=channels, since=since)

    def validate(self):
        valid = True
        unique_metrics = set()
        for name, test in self.__conf.tests.items():
            logging.info("Checking {}".format(name))
            test_metrics = test.fully_qualified_metric_names()
            for test_metric in test_metrics:
                if test_metric not in unique_metrics:
                    unique_metrics.add(test_metric)
                else:
                    valid = False
                    logging.error(f"Found duplicated metric: {test_metric}")
            try:
                importer = self.__importers.get(test)
                series = importer.fetch_data(test)
                for metric, metric_data in series.data.items():
                    if not metric_data:
                        logging.warning(f"Test's metric does not have data: {name} {metric}")
            except Exception as err:
                logging.error(f"Invalid test definition: {name}\n{repr(err)}\n")
                valid = False
        logging.info(f"Validation finished: {'VALID' if valid else 'INVALID'}")
        if not valid:
            exit(1)


def setup_data_selector_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--branch", metavar="STRING", dest="branch", help="name of the branch", nargs="?"
    )
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
        help="the commit at the start of the time span to analyze",
    )
    since_group.add_argument(
        "--since-version",
        metavar="STRING",
        dest="since_version",
        help="the version at the start of the time span to analyze",
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
        help="the commit at the end of the time span to analyze",
    )
    until_group.add_argument(
        "--until-version",
        metavar="STRING",
        dest="until_version",
        help="the version at the end of the time span to analyze",
    )
    until_group.add_argument(
        "--until",
        metavar="DATE",
        dest="until_time",
        help="the end of the time span to analyze; same syntax as --since",
    )
    parser.add_argument(
        "--last",
        type=int,
        metavar="COUNT",
        dest="last_n_points",
        help="the number of data points to take from the end of the series",
    )


def data_selector_from_args(args: argparse.Namespace) -> DataSelector:
    data_selector = DataSelector()
    if args.branch:
        data_selector.branch = args.branch
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
    if args.last_n_points is not None:
        data_selector.last_n_points = args.last_n_points
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
    parser.add_argument(
        "--orig-edivisive",
        type=bool,
        default=False,
        dest="orig_edivisive",
        help="use the original edivisive algorithm with no windowing "
        "and weak change points analysis improvements",
    )
    parser.add_argument(
        "--output",
        help="Output format for the generated report.",
        choices=list(ReportType),
        dest="report_type",
        default=ReportType.LOG,
        type=ReportType,
    )


def analysis_options_from_args(args: argparse.Namespace) -> AnalysisOptions:
    conf = AnalysisOptions()
    if args.pvalue is not None:
        conf.max_pvalue = args.pvalue
    if args.magnitude is not None:
        conf.min_magnitude = args.magnitude
    if args.window is not None:
        conf.window_len = args.window
    if args.orig_edivisive is not None:
        conf.orig_edivisive = args.orig_edivisive
    return conf


def main():
    try:
        conf = config.load_config()
    except ConfigError as err:
        logging.error(err.message)
        exit(1)
    script_main(conf)


def script_main(conf: Config, args: List[str] = None):
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
        "--update-postgres",
        help="Update PostgreSQL database results with change points",
        action="store_true",
    )
    analyze_parser.add_argument(
        "--update-bigquery",
        help="Update BigQuery database results with change points",
        action="store_true",
    )
    analyze_parser.add_argument(
        "--notify-slack",
        help="Send notification containing a summary of change points to given Slack channels",
        nargs="+",
    )
    analyze_parser.add_argument(
        "--cph-report-since",
        help="Sets a limit on the date range of the Change Point History reported to Slack. Same syntax as --since.",
        metavar="DATE",
        dest="cph_report_since",
    )
    setup_data_selector_parser(analyze_parser)
    setup_analysis_options_parser(analyze_parser)

    regressions_parser = subparsers.add_parser("regressions", help="find performance regressions")
    regressions_parser.add_argument(
        "tests", help="name of the test or group of the tests", nargs="+"
    )
    regressions_parser.add_argument(
        "--ignore-direction",
        help="ignore the direction of the change in performance",
        dest="ignore_direction",
        action="store_true",
    )
    setup_data_selector_parser(regressions_parser)
    setup_analysis_options_parser(regressions_parser)

    remove_annotations_parser = subparsers.add_parser("remove-annotations")
    remove_annotations_parser.add_argument(
        "tests", help="name of the test or test group", nargs="*"
    )
    remove_annotations_parser.add_argument(
        "--force", help="don't ask questions, just do it", dest="force", action="store_true"
    )

    subparsers.add_parser(
        "validate", help="validates the tests and metrics defined in the configuration"
    )

    try:
        args = parser.parse_args(args=args)
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
            update_postgres_flag = args.update_postgres
            update_bigquery_flag = args.update_bigquery
            slack_notification_channels = args.notify_slack
            slack_cph_since = parse_datetime(args.cph_report_since)
            data_selector = data_selector_from_args(args)
            options = analysis_options_from_args(args)
            report_type = args.report_type
            tests = hunter.get_tests(*args.tests)
            tests_analyzed_series = {test.name: None for test in tests}
            for test in tests:
                try:
                    analyzed_series = hunter.analyze(
                        test, selector=data_selector, options=options, report_type=report_type
                    )
                    if update_grafana_flag:
                        if not isinstance(test, GraphiteTestConfig):
                            raise GrafanaError("Not a Graphite test")
                        hunter.update_grafana_annotations(test, analyzed_series)
                    if update_postgres_flag:
                        if not isinstance(test, PostgresTestConfig):
                            raise PostgresError("Not a Postgres test")
                        hunter.update_postgres(test, analyzed_series)
                    if update_bigquery_flag:
                        if not isinstance(test, BigQueryTestConfig):
                            raise BigQueryError("Not a BigQuery test")
                        hunter.update_bigquery(test, analyzed_series)
                    if slack_notification_channels:
                        tests_analyzed_series[test.name] = analyzed_series
                except DataImportError as err:
                    logging.error(err.message)
                except GrafanaError as err:
                    logging.error(
                        f"Failed to update grafana dashboards for {test.name}: {err.message}"
                    )
                except PostgresError as err:
                    logging.error(
                        f"Failed to update postgres database for {test.name}: {err.message}"
                    )
            if slack_notification_channels:
                hunter.notify_slack(
                    tests_analyzed_series,
                    selector=data_selector,
                    channels=slack_notification_channels,
                    since=slack_cph_since,
                )

        if args.command == "regressions":
            data_selector = data_selector_from_args(args)
            options = analysis_options_from_args(args)
            tests = hunter.get_tests(*args.tests)
            regressing_test_count = 0
            errors = 0
            for test in tests:
                try:
                    regressions = hunter.regressions(
                        test,
                        selector=data_selector,
                        options=options,
                        ignore_direction=args.ignore_direction,
                        report_type=args.report_type,
                    )
                    if regressions:
                        regressing_test_count += 1
                except HunterError as err:
                    logging.error(err.message)
                    errors += 1
                except DataImportError as err:
                    logging.error(err.message)
                    errors += 1

            if args.report_type == ReportType.LOG:
                if regressing_test_count == 0:
                    print("No regressions found!")
                elif regressing_test_count == 1:
                    print("Regressions in 1 test found")
                else:
                    print(f"Regressions in {regressing_test_count} tests found")

            if errors > 0:
                print("Some tests were skipped due to import / analyze errors. Consult error log.")

        if args.command == "remove-annotations":
            if args.tests:
                tests = hunter.get_tests(*args.tests)
                for test in tests:
                    hunter.remove_grafana_annotations(test, args.force)
            else:
                hunter.remove_grafana_annotations(None, args.force)

        if args.command == "validate":
            hunter.validate()

        if args.command is None:
            parser.print_usage()

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
