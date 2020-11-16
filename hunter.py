from hunter import config

import argparse

from hunter.config import ConfigError
from hunter.fallout import Fallout, FalloutError
from hunter.graphite import Graphite
from hunter.importer import FalloutImporter
from hunter.util import eprint, remove_common_prefix


def list_tests(fallout: Fallout):
    for test_name in fallout.list_tests(user):
        print(test_name)


def analyze_runs(fallout: Fallout, graphite: Graphite):
    results = FalloutImporter(fallout, graphite).fetch(args.test, args.user)
    print("Test Runs:")
    print(results.format_log())
    print("Change Points:")
    print(results.format_change_points())


parser = argparse.ArgumentParser(
    description="Hunts performance regressions in Fallout results")
parser.add_argument("--user", help="user-name in Fallout")

subparsers = parser.add_subparsers(dest="command")
list_parser = subparsers.add_parser("list", help="list available tests")
analyze_parser = subparsers.add_parser("analyze", help="analyze pperformance test results")
analyze_parser.add_argument("test", help="name of the test in Fallout")

try:
    args = parser.parse_args()
    user = args.user
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

