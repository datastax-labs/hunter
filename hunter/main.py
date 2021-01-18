import argparse
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import dateparser
import pystache

from hunter import config
from hunter.config import ConfigError
from hunter.fallout import Fallout, FalloutError
from hunter.grafana import Annotation, Grafana, GrafanaError
from hunter.graphite import Graphite, GraphiteError, DataSelector
from hunter.importer import FalloutImporter, DataImportError
from hunter.report import Report


def setup():
    fallout_user = input("Fallout user name (email): ")
    fallout_token = input("Fallout token: ")
    conf_template = (Path(__file__).parent / "resources" / "conf.yaml.template").read_text()
    conf_yaml = pystache.render(conf_template, {
        'fallout_token': fallout_token,
        'fallout_user': fallout_user
    })
    hunter_conf_dir = (Path.home() / ".hunter")
    if not hunter_conf_dir.exists():
        hunter_conf_dir.mkdir()
    os.umask(0o600) # Don't share credentials with other users
    (Path.home() / ".hunter" / "conf.yaml").write_text(conf_yaml)
    exit(0)


def list_tests(fallout: Fallout, user: Optional[str]):
    for test_name in fallout.list_tests(user):
        print(test_name)
    exit(0)


def analyze_runs(
        fallout: Fallout,
        graphite: Graphite,
        test: str,
        user: Optional[str],
        selector: DataSelector):
    results = FalloutImporter(fallout, graphite).fetch(test, user, selector)
    results.find_change_points()

    report = Report(results)
    print(report.format_log_annotated())
    exit(0)


def update_grafana(fallout: Fallout,
                   graphite: Graphite,
                   grafana: Grafana,
                   test: str,
                   user: Optional[str],
                   selector: DataSelector):
    results = FalloutImporter(fallout, graphite).fetch(test, user, selector)
    results.find_change_points()

    logging.info("Determining new Grafana annotations...")
    annotations = []
    for change_point in results.change_points:
        for change in change_point.changes:
            metric = change.metric
            relevant_dashboard_panels = grafana.find_all_dashboard_panels_displaying(metric)
            for dashboard_panel in relevant_dashboard_panels:
                # Grafana timestamps have 13 digits, Graphite timestamps have 10 (hence multiplication by 10^3)
                # TODO: Replace text field with Fallout test run URL, eventually
                annotations.append(
                    Annotation(
                        dashboard_id=dashboard_panel["dashboard id"],
                        panel_id=dashboard_panel["panel id"],
                        time=change.time * 10**3,
                        text=metric,
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
    exit(0)


@dataclass
class DateFormatError(ValueError):
    message: str


def parse_date(date: Optional[str]) -> Optional[datetime]:
    if date is None:
        return None
    parsed = dateparser.parse(date)
    if parsed is None:
        raise DateFormatError(f"Invalid date: {date}")
    return parsed


def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Hunts performance regressions in Fallout results")
    parser.add_argument("--user", help="user-name in Fallout")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("setup", help="run interactive setup")
    subparsers.add_parser("list", help="list available tests")
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="analyze performance test results",
        formatter_class=argparse.RawTextHelpFormatter)
    analyze_parser.add_argument("test", help="name of the test in Fallout")
    analyze_parser.add_argument(
        "--metrics",
        dest="metrics",
        help="comma separated list of metrics to analyze")
    analyze_parser.add_argument(
        "--from",
        dest="from_time",
        help="the start of the time span to analyze; "
             "accepts ISO, and human-readable dates like '10 weeks ago'")
    analyze_parser.add_argument(
        "--until",
        dest="until_time",
        help="the end of the time span to analyze; same syntax as --from")
    update_grafana_parser = subparsers.add_parser(
        "update_grafana",
        help="analyze performance test results, update relevant Grafana charts with annotations "
             "for determined changepoints")
    update_grafana_parser.add_argument("test", help="name of the test in Fallout")
    update_grafana_parser.add_argument(
        "--metrics",
        dest="metrics",
        help="comma separated list of metrics to analyze")
    update_grafana_parser.add_argument(
        "--from",
        dest="from_time",
        help="the start of the time span to analyze; "
             "accepts ISO, and human-readable dates like '10 weeks ago'")
    update_grafana_parser.add_argument(
        "--until",
        dest="until_time",
        help="the end of the time span to analyze; same syntax as --from")

    try:
        args = parser.parse_args()
        user = args.user

        if args.command == "setup":
            setup()

        conf = config.load_config()
        fallout = Fallout(conf.fallout)
        graphite = Graphite(conf.graphite)

        if args.command == "list":
            list_tests(fallout, user)
        if args.command == "analyze":
            data_selector = DataSelector()
            if args.metrics is not None:
                data_selector.metrics = list(args.metrics.split(","))
            data_selector.from_time = parse_date(args.from_time)
            data_selector.until_time = parse_date(args.until_time)
            analyze_runs(fallout, graphite, args.test, user, data_selector)
        if args.command == "update_grafana":
            grafana = Grafana(conf.grafana)
            data_selector = DataSelector()
            if args.metrics is not None:
                data_selector.metrics = list(args.metrics.split(","))
            data_selector.from_time = parse_date(args.from_time)
            data_selector.until_time = parse_date(args.until_time)
            update_grafana(fallout, graphite, grafana, args.test, user, data_selector)
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
