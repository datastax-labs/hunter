import argparse
import logging
import os
from pathlib import Path
from typing import Dict, Optional, List

import pystache

from hunter import config
from hunter.config import ConfigError, Config
from hunter.csv import CsvOptions
from hunter.data_selector import DataSelector
from hunter.attributes import get_html_from_attributes
from hunter.fallout import Fallout, FalloutError
from hunter.grafana import Annotation, Grafana, GrafanaError
from hunter.graphite import Graphite, GraphiteError
from hunter.importer import get_importer, FalloutImporter, DataImportError
from hunter.series import Series, AnalysisOptions, ChangePointGroup
from hunter.report import Report
from hunter.test_config import create_test_config, TestConfigError, TestGroup, TestGroupError
from hunter.util import parse_datetime, DateFormatError


def list_tests(conf: Config, user: Optional[str]):
    fallout = Fallout(conf.fallout)
    for test_name in fallout.list_tests(user):
        print(test_name)
    exit(0)


def list_metrics(conf: Config, csv_options: CsvOptions, test: str, user: Optional[str]):
    test_info = {"name": test, "user": user, "suffixes": conf.graphite.suffixes}
    test_conf = create_test_config(test_info, csv_options)
    importer = get_importer(test_conf, conf)
    for metric_name in importer.fetch_all_metric_names(test_conf):
        print(metric_name)
    exit(0)


def analyze_runs(
    conf: Config,
    csv_options: CsvOptions,
    test: str,
    user: Optional[str],
    selector: DataSelector,
    analysis_options: AnalysisOptions,
    update_grafana_flag: bool,
):

    test_info = {"name": test, "user": user, "suffixes": conf.graphite.suffixes}
    test_conf = create_test_config(test_info, csv_options)
    importer = get_importer(test_conf, conf)
    series = importer.fetch(test_conf, selector)
    change_points = series.all_change_points(analysis_options)

    # update Grafana first, so that associated logging messages not last to be printed to stdout
    if update_grafana_flag:
        if isinstance(importer, FalloutImporter):
            grafana = Grafana(conf.grafana)
            update_grafana(series.test_name, change_points, importer.fallout, grafana)
        else:
            logging.warning("Provided test is not compatible with Grafana updates")

    report = Report(series, change_points)
    print(report.format_log_annotated())
    exit(0)


def bulk_analyze_runs(
    conf: Config,
    test_group_file: str,
    user: Optional[str],
    selector: DataSelector,
    analysis_options: AnalysisOptions,
    update_grafana_flag: bool,
):
    grafana = Grafana(conf.grafana) if update_grafana_flag else None

    test_group = TestGroup(Path(test_group_file), user)
    change_points = {}
    perf_tests = {}
    for test_name, test_conf in test_group.test_configs.items():
        importer = get_importer(test_conf=test_conf, conf=conf)
        series = importer.fetch(test_conf, selector)
        perf_tests[test_name] = series
        change_points[test_name] = series.all_change_points(analysis_options)
        if grafana is not None and isinstance(importer, FalloutImporter):
            update_grafana(test_name, change_points[test_name], importer.fallout, grafana)

    # TODO: Improve this output
    for test_name, series in perf_tests.items():
        report = Report(series, change_points[test_name])
        print(f"\n{test_name}")
        print(report.format_log_annotated())
    exit(0)


def update_grafana(
    test_name: str, change_points: List[ChangePointGroup], fallout: Fallout, grafana: Grafana
):
    logging.info(f"Determining new Grafana annotations for test {test_name}...")
    annotations = []
    for change_point in change_points:
        annotation_text = get_html_from_attributes(
            test_name=test_name, attributes=change_point.attributes, fallout=fallout
        )
        for change in change_point.changes:
            matching_dashboard_panels = grafana.find_all_dashboard_panels_displaying(change.metric)
            for dashboard_panel in matching_dashboard_panels:
                # Grafana timestamps have 13 digits, Graphite timestamps have 10
                # (hence multiplication by 10^3)
                annotations.append(
                    Annotation(
                        dashboard_id=dashboard_panel["dashboard id"],
                        panel_id=dashboard_panel["panel id"],
                        time=change.time * 10 ** 3,
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


def setup_csv_options_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--csv-delimiter",
        metavar="CHAR",
        dest="csv_delimiter",
        default=",",
        help="CSV column separator [default: ',']",
    )
    parser.add_argument(
        "--csv-quote",
        metavar="CHAR",
        dest="csv_quote_char",
        default='"',
        help="CSV value quote character [default: '\"']",
    )
    parser.add_argument(
        "--csv-time-column",
        metavar="COLUMN",
        dest="csv_time_column",
        help="Name of the column storing the timestamp of each run; "
        "if not given, hunter will try to autodetect from value types",
    )


def csv_options_from_args(args: argparse.Namespace) -> CsvOptions:
    csv_options = CsvOptions()
    if args.csv_delimiter is not None:
        csv_options.delimiter = args.csv_delimiter
    if args.csv_quote_char is not None:
        csv_options.quote_char = args.csv_quote_char
    if args.csv_time_column is not None:
        csv_options.time_column = args.csv_time_column
    return csv_options


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
    parser.add_argument("--user", help="user-name in Fallout")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("list-tests", help="list available tests")

    list_metrics_parser = subparsers.add_parser(
        "list-metrics", help="list available metrics collected for a test"
    )
    list_metrics_parser.add_argument(
        "test", help="name of the test in Fallout or local path to CSV file"
    )
    setup_csv_options_parser(list_metrics_parser)

    analyze_parser = subparsers.add_parser(
        "analyze",
        help="analyze performance test results",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    analyze_parser.add_argument(
        "test", help="name of the test in Fallout or path to a CSV file with data"
    )
    analyze_parser.add_argument(
        "--update-grafana",
        help="Update Grafana dashboards with appropriate annotations of change points",
        action="store_true",
    )

    setup_data_selector_parser(analyze_parser)
    setup_csv_options_parser(analyze_parser)
    setup_analysis_options_parser(analyze_parser)

    bulk_analyze_parser = subparsers.add_parser(
        "bulk-analyze",
        help="analyze a specified list of performance tests",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    bulk_analyze_parser.add_argument(
        "test_group", help="path to yaml file that stores list of tests to analyze"
    )
    bulk_analyze_parser.add_argument(
        "--update-grafana",
        help="Update Grafana dashboards with appropriate annotations of change points",
        action="store_true",
    )
    setup_data_selector_parser(bulk_analyze_parser)
    setup_analysis_options_parser(bulk_analyze_parser)

    try:
        args = parser.parse_args()
        user = args.user

        conf = config.load_config()
        if args.command == "list-tests":
            list_tests(conf, user)
        if args.command == "list-metrics":
            csv_options = csv_options_from_args(args)
            list_metrics(conf, csv_options, args.test, user)
        if args.command == "analyze":
            csv_options = csv_options_from_args(args)
            data_selector = data_selector_from_args(args)
            analysis_options = analysis_options_from_args(args)
            update_grafana_flag = args.update_grafana
            analyze_runs(
                conf,
                csv_options,
                args.test,
                user,
                data_selector,
                analysis_options,
                update_grafana_flag,
            )
        if args.command == "bulk-analyze":
            data_selector = data_selector_from_args(args)
            update_grafana_flag = args.update_grafana
            analysis_options = analysis_options_from_args(args)
            bulk_analyze_runs(
                conf, args.test_group, user, data_selector, analysis_options, update_grafana_flag
            )
        if args.command is None:
            parser.print_usage()

    except ConfigError as err:
        logging.error(err.message)
        exit(1)
    except TestConfigError as err:
        logging.error(err.message)
        exit(1)
    except TestGroupError as err:
        logging.error(err.message)
        exit(1)
    except FalloutError as err:
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
    except DateFormatError as err:
        logging.error(err.message)
        exit(1)


if __name__ == "__main__":
    main()
