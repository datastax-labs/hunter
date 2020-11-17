from hunter import config

import argparse

import argparse
import os
from pathlib import Path

import pystache

from hunter import config
from hunter.config import ConfigError
from hunter.fallout import Fallout, FalloutError
from hunter.graphite import Graphite
from hunter.importer import FalloutImporter
from hunter.util import eprint


def setup():
    fallout_user = input("Fallout user name (email): ")
    fallout_token = input("Fallout token: ")
    conf_template = (Path(__file__).parent / "resources" / "conf.yaml.template").read_text()
    conf_yaml = pystache.render(conf_template, {
        'fallout_token': fallout_token,
        'fallout_user': fallout_user
    })
    os.umask(0o600) # Don't share credentials with other users
    (Path.home() / ".hunter" / "conf.yaml").write_text(conf_yaml)
    exit(0)


def list_tests(fallout: Fallout):
    for test_name in fallout.list_tests(user):
        print(test_name)
    exit(0)


def analyze_runs(fallout: Fallout, graphite: Graphite):
    results = FalloutImporter(fallout, graphite).fetch(args.test, args.user)
    print("Test Runs:")
    print(results.format_log_annotated())
    print()
    print("Change Points:")
    print(results.format_change_points())
    exit(0)

def main():
    parser = argparse.ArgumentParser(
        description="Hunts performance regressions in Fallout results")
    parser.add_argument("--user", help="user-name in Fallout")

    subparsers = parser.add_subparsers(dest="command")
    setup_parser = subparsers.add_parser("setup", help="run interactive setup")
    list_parser = subparsers.add_parser("list", help="list available tests")
    analyze_parser = subparsers.add_parser("analyze", help="analyze performance test results")
    analyze_parser.add_argument("test", help="name of the test in Fallout")

    try:
        args = parser.parse_args()
        user = args.user

        if args.command == "setup":
            setup()

        conf = config.load_config()
        fallout = Fallout(conf.fallout)
        graphite = Graphite(conf.graphite)

        if args.command == "list":
            list_tests(fallout)
        if args.command == "analyze":
            analyze_runs(fallout, graphite)
        if args.command is None:
            parser.print_usage()

    except ConfigError as err:
        eprint(err.message)
        exit(1)
    except FalloutError as err:
        eprint(err.message)
        exit(1)


if __name__ == "__main__":
    main()
