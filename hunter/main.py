import argparse
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pystache
import pytz

from hunter import config
from hunter.config import ConfigError, Config
from hunter.data_selector import DataSelector
from hunter.event_processor import EventProcessor
from hunter.fallout import Fallout, FalloutError
from hunter.grafana import Annotation, Grafana, GrafanaError
from hunter.graphite import Graphite, GraphiteError
from hunter.importer import FalloutImporter, DataImportError, CsvImporter, \
    CsvOptions
from hunter.performance_test import CsvPerformanceTest, FalloutPerformanceTest, FalloutPerformanceTestGroup
from hunter.util import parse_datetime, DateFormatError


def setup():
    fallout_user = input("Fallout user name (email): ")
    fallout_token = input("Fallout token: ")
    conf_template = \
        (Path(__file__).parent / "resources" / "conf.yaml.template").read_text()
    conf_yaml = pystache.render(conf_template, {
        'fallout_token': fallout_token,
        'fallout_user': fallout_user
    })

    test_group_template = (Path(__file__).parent / "resources" / "test_group.yaml.template").read_text()
    test_group_yaml = pystache.render(test_group_template)

    hunter_conf_dir = (Path.home() / ".hunter")
    if not hunter_conf_dir.exists():
        hunter_conf_dir.mkdir()
    os.umask(0o600) # Don't share credentials with other users
    (Path.home() / ".hunter" / "conf.yaml").write_text(conf_yaml)
    (Path.home() / ".hunter" / "test_group.yaml").write_text(test_group_yaml)
    exit(0)


def list_tests(conf: Config, user: Optional[str]):
    fallout = Fallout(conf.fallout)
    for test_name in fallout.list_tests(user):
        print(test_name)
    exit(0)


def list_metrics(conf: Config, test: str, user: Optional[str]):
    fallout = Fallout(conf.fallout)
    graphite = Graphite(conf.graphite)
    importer = FalloutImporter(fallout, graphite)
    for metric_path in importer.fetch_metric_paths(test, user):
        print(metric_path)
    exit(0)


def list_suffixes(conf: Config, test: str, user: Optional[str]):
    fallout = Fallout(conf.fallout)
    graphite = Graphite(conf.graphite)
    importer = FalloutImporter(fallout, graphite)
    for suffix in importer.fetch_suffixes(test, user):
        print(suffix)
    exit(0)


def analyze_runs(
        conf: Config,
        csv_options: CsvOptions,
        test: str,
        user: Optional[str],
        selector: DataSelector,
        update_grafana_flag: bool):

    if test.lower().endswith("csv"):
        importer = CsvImporter(csv_options)
        perf_test = CsvPerformanceTest(Path(test), importer, selector)
    else:
        fallout = Fallout(conf.fallout)
        graphite = Graphite(conf.graphite)
        importer = FalloutImporter(fallout, graphite)
        if conf.graphite.suffixes:
            selector.suffixes = conf.graphite.suffixes
        else:
            selector.suffixes = importer.fetch_suffixes(test, user)
        perf_test = FalloutPerformanceTest(test, user, importer, selector)

    perf_test.fetch()
    perf_test.find_change_points()

    # update Grafana first, so that associated logging messages are not the last to be printed to stdout
    if update_grafana_flag and type(importer) == FalloutImporter:
        grafana = Grafana(conf.grafana)
        update_grafana(perf_test, importer.fallout, importer.graphite, grafana)

    report = perf_test.get_report()
    print(report.format_log_annotated())
    exit(0)


def bulk_analyze_runs(
        conf: Config,
        test_group_file: str,
        user: Optional[str],
        selector: DataSelector,
        update_grafana_flag: bool):
    fallout = Fallout(conf.fallout)
    graphite = Graphite(conf.graphite)
    importer = FalloutImporter(fallout, graphite)
    perf_test_group = FalloutPerformanceTestGroup(Path(test_group_file), user, importer, selector)
    perf_test_group.fetch()
    perf_test_group.find_change_points()

    if update_grafana_flag:
        grafana = Grafana(conf.grafana)
        for test_name, perf_test in perf_test_group.performance_tests.items():
            update_grafana(perf_test, fallout, graphite, grafana)

    report_dict = perf_test_group.get_reports()
    # TODO: Improve this output
    for test_name, report in report_dict.items():
        print(test_name)
        print(report.format_log_annotated())


def update_grafana(perf_test: FalloutPerformanceTest, fallout: Fallout, graphite: Graphite, grafana: Grafana):
    logging.info("Determining new Grafana annotations...")
    annotations = []
    event_processor = EventProcessor(fallout, graphite)
    for change_point in perf_test.performance_log.change_points:
        for change in change_point.changes:
            relevant_dashboard_panels = grafana.find_all_dashboard_panels_displaying(change.metric)
            if len(relevant_dashboard_panels) > 0:
                # determine Fallout and GitHub hyperlinks for displaying in annotation
                annotation_text = event_processor.get_html_from_test_run_event(
                    test_name=perf_test.test_name,
                    timestamp=datetime.fromtimestamp(change.time, tz=pytz.UTC)
                )
                for dashboard_panel in relevant_dashboard_panels:
                    # Grafana timestamps have 13 digits, Graphite timestamps have 10 (hence multiplication by 10^3)
                    annotations.append(
                        Annotation(
                            dashboard_id=dashboard_panel["dashboard id"],
                            panel_id=dashboard_panel["panel id"],
                            time=change.time * 10**3,
                            text=annotation_text,
                            tags=dashboard_panel["tags"]
                        )
                    )
    if len(annotations) == 0:
        logging.info("No Grafana panels to update")
    else:
        logging.info("Updating Grafana with latest annotations...")
        for annotation in annotations:
            # remove any existing annotations that have the same dashboard id, panel id, and set of tags
            grafana.delete_matching_annotations(annotation=annotation)
        for annotation in annotations:
            grafana.post_annotation(annotation)


def setup_csv_options_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--csv-delimiter",
        metavar="CHAR",
        dest="csv_delimiter",
        default=",",
        help="CSV column separator [default: ',']")
    parser.add_argument(
        "--csv-quote",
        metavar="CHAR",
        dest="csv_quote_char",
        default='"',
        help="CSV value quote character [default: '\"']")
    parser.add_argument(
        "--csv-time-column",
        metavar="COLUMN",
        dest="csv_time_column",
        help="Name of the column storing the timestamp of each run; "
             "if not given, hunter will try to autodetect from value types")


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
        help="a comma-separated list of metrics to analyze")
    parser.add_argument(
        "--attrs",
        metavar="LIST",
        dest="attributes",
        help="a comma-separated list of attribute names associated with the runs "
             "(e.g. commit, branch, version); "
             "if not specified, it will be automatically filled based on available information")
    parser.add_argument(
        "--from",
        metavar="DATE",
        dest="from_time",
        help="the start of the time span to analyze; "
             "accepts ISO, and human-readable dates like '10 weeks ago'")
    parser.add_argument(
        "--until",
        metavar="DATE",
        dest="until_time",
        help="the end of the time span to analyze; same syntax as --from")


def data_selector_from_args(args: argparse.Namespace) -> DataSelector:
    data_selector = DataSelector()
    if args.metrics is not None:
        data_selector.metrics = list(args.metrics.split(","))
    if args.attributes is not None:
        data_selector.attributes = list(args.attributes.split(","))
    data_selector.from_time = parse_datetime(args.from_time)
    data_selector.until_time = parse_datetime(args.until_time)
    return data_selector


def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Hunts performance regressions in Fallout results")
    parser.add_argument("--user", help="user-name in Fallout")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("setup", help="run interactive setup")
    subparsers.add_parser("list_tests", help="list available tests")

    list_metrics_parser = subparsers.add_parser("list_metrics", help="list available metrics collected for a test")
    list_metrics_parser.add_argument(
        "test",
        help="name of the test in Fallout"
    )

    list_suffixes_parser = subparsers.add_parser("list_suffixes", help="")
    list_suffixes_parser.add_argument(
        "test",
        help="name of the test in Fallout"
    )

    analyze_parser = subparsers.add_parser(
        "analyze",
        help="analyze performance test results",
        formatter_class=argparse.RawTextHelpFormatter)
    analyze_parser.add_argument(
        "test",
        help="name of the test in Fallout or path to a CSV file with data")
    analyze_parser.add_argument('--update-grafana',
                                help='Update Grafana dashboards with appropriate annotations of change points',
                                action="store_true")
    setup_data_selector_parser(analyze_parser)
    setup_csv_options_parser(analyze_parser)

    bulk_analyze_parser = subparsers.add_parser(
        "bulk_analyze",
        help="analyze a specified list of performance tests",
        formatter_class=argparse.RawTextHelpFormatter)
    bulk_analyze_parser.add_argument(
        "test_group",
        help = "path to yaml file that stores list of tests to analyze")
    bulk_analyze_parser.add_argument('--update-grafana',
                                help = 'Update Grafana dashboards with appropriate annotations of change points',
                                action = "store_true")
    setup_data_selector_parser(bulk_analyze_parser)

    try:
        args = parser.parse_args()
        user = args.user

        if args.command == "setup":
            setup()

        conf = config.load_config()
        if args.command == "list_tests":
            list_tests(conf, user)
        if args.command == "list_metrics":
            list_metrics(conf, args.test, user)
        if args.command == "list_suffixes":
            list_suffixes(conf, args.test, user)
        if args.command == "analyze":
            csv_options = csv_options_from_args(args)
            data_selector = data_selector_from_args(args)
            update_grafana_flag = args.update_grafana
            analyze_runs(conf, csv_options, args.test, user, data_selector, update_grafana_flag)
        if args.command == "bulk_analyze":
            data_selector = data_selector_from_args(args)
            update_grafana_flag = args.update_grafana
            bulk_analyze_runs(conf, args.test_group, user, data_selector, update_grafana_flag)
        if args.command is None:
            parser.print_usage()

    except ConfigError as err:
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
