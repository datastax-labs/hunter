import ast
import json
import urllib.request

from dataclasses import dataclass
from datetime import datetime
from logging import info, warning
from typing import Dict, List, Optional

from hunter.data_selector import DataSelector
from hunter.util import parse_datetime


@dataclass
class GraphiteConfig:
    url: str
    suffixes: List[str]


@dataclass
class DataPoint:
    time: int
    value: float


@dataclass
class TimeSeries:
    name: str
    points: List[DataPoint]


def decode_graphite_datapoints(series: Dict[str, List[List[float]]]) -> List[DataPoint]:

    points = series["datapoints"]
    return [DataPoint(int(p[1]), p[0]) for p in points if p[0] is not None]


def to_graphite_time(time: datetime, default: str) -> str:
    """
    Note that millissecond-level precision matters when trying to fetch events in a given time
    range, hence opting for this over time.strftime("%H:%M_%Y%m%d")
    """
    if time is not None:
        return str(int(time.timestamp()))
    else:
        return default


@dataclass
class GraphiteError(IOError):
    message: str


@dataclass
class GraphiteEvent:
    test_owner: str
    test_name: str
    run_id: str
    status: str
    start_time: datetime
    pub_time: datetime
    end_time: datetime
    version: Optional[str]
    branch: Optional[str]
    commit: Optional[str]

    def __init__(
        self,
        pub_time: int,
        test_owner: str,
        test_name: str,
        run_id: str,
        status: str,
        start_time: int,
        end_time: int,
        version: Optional[str],
        branch: Optional[str],
        commit: Optional[str],
    ):
        self.test_owner = test_owner
        self.test_name = test_name
        self.run_id = run_id
        self.status = status
        self.start_time = parse_datetime(str(start_time))
        self.pub_time = parse_datetime(str(pub_time))
        self.end_time = parse_datetime(str(end_time))
        if len(version) == 0 or version == "null":
            self.version = None
        else:
            self.version = version
        if len(branch) == 0 or branch == "null":
            self.branch = None
        else:
            self.branch = branch
        if len(commit) == 0 or commit == "null":
            self.commit = None
        else:
            self.commit = commit


class Graphite:
    __url: str
    __url_limit: int  # max URL length used when requesting metrics from Graphite

    def __init__(self, conf: GraphiteConfig):
        self.__url = conf.url
        self.__url_limit = 4094

    def fetch_events(
        self,
        tags: List[str],
        from_time: Optional[datetime] = None,
        until_time: Optional[datetime] = None,
    ) -> List[GraphiteEvent]:
        """
        Returns 'Performance Test' events that match all of
        the following criteria:
        - all tags passed in match
        - published between given from_time and until_time (both bounds inclusive)

        References:
            - Graphite events REST API: https://graphite.readthedocs.io/en/stable/events.html
            - Haxx: https://github.com/riptano/haxx/pull/588
        """
        try:
            from_time = to_graphite_time(from_time, "-365d")
            until_time = to_graphite_time(until_time, "now")

            url = (
                f"{self.__url}events/get_data"
                f"?tags={'+'.join(tags)}"
                f"&from={from_time}"
                f"&until={until_time}"
                f"&set=intersection"
            )
            data_str = urllib.request.urlopen(url).read()
            data_as_json = json.loads(data_str)
            return [
                GraphiteEvent(event.get("when"), **ast.literal_eval(event.get("data")))
                for event in data_as_json
                if event.get("what") == "Performance Test"
            ]

        except IOError as e:
            raise GraphiteError(f"Failed to fetch Graphite events: {str(e)}")

    def fetch_events_with_matching_time_option(
        self, fallout_user: str, test_name: str, commit: Optional[str], version: Optional[str]
    ) -> List[GraphiteEvent]:
        events = []
        if commit is not None:
            tags = [fallout_user, test_name]
            events = list(filter(lambda e: e.commit == commit, self.fetch_events(tags)))
            # the test of interest was not run against commit of interest, we search all tests
            # to see if any were run against this commit
            if len(events) == 0:
                tags = [fallout_user]
                events = list(
                    filter(
                        lambda e: e.commit == commit,
                        self.fetch_events(tags),
                    )
                )
        elif version is not None:
            tags = [fallout_user, test_name, version]
            events = self.fetch_events(tags)
            # the test of interest was not run against the version of interest, we search
            # all tests to see any were run against this version
            if len(events) == 0:
                tags = [fallout_user, version]
                events = self.fetch_events(tags)
        return events

    def fetch_data(
        self, prefix: str, suffixes: List[str], selector: DataSelector
    ) -> List[TimeSeries]:
        """
        Connects to Graphite server and downloads interesting series with the
        given prefix. The series to be downloaded are picked from SUFFIXES list.
        """
        try:
            info("Fetching data from Graphite...")
            result = []
            if selector.metrics is not None:
                metrics = "{" + ",".join(selector.metrics) + "}"
            else:
                metrics = "*"
            from_time = to_graphite_time(selector.since_time, "-365d")
            until_time = to_graphite_time(selector.until_time, "now")

            data_as_json = []
            targets = ""
            for suffix in suffixes:
                url = (
                    f"{self.__url}render"
                    f"?{targets.strip('&')}"
                    f"&format=json"
                    f"&from={from_time}"
                    f"&until={until_time}"
                )
                new_target = f"target={prefix}.{suffix}.{metrics}&"
                # if adding new_target overflows URL limit, send request with current targets
                if len(url) + len(new_target) > self.__url_limit:
                    data_str = urllib.request.urlopen(url).read()
                    data_as_json += json.loads(data_str)
                    targets = ""
                targets += new_target
            # request data for remaining targets
            data_str = urllib.request.urlopen(url).read()
            data_as_json += json.loads(data_str)

            for s in data_as_json:
                series = TimeSeries(name=s["target"], points=decode_graphite_datapoints(s))
                if len(series.points) > 5:
                    result.append(series)
                else:
                    warning(
                        f"Not enough data points in series {series.name}. "
                        f"Required at least 5 points."
                    )

            return result

        except IOError as err:
            raise GraphiteError(f"Failed to fetch data from Graphite: {str(err)}")

    def fetch_metric_paths(self, prefix: str, paths: Optional[List[str]] = None) -> List[str]:
        """
        Provided a valid Graphite metric prefix, this method will retrieve all corresponding metric paths
        Reference:
        - https://graphite-api.readthedocs.io/en/latest/api.html
        """
        if paths is None:
            paths = []
        try:
            url = f"{self.__url}metrics/find?query={prefix}"
            data_str = urllib.request.urlopen(url).read()
            data_as_json = json.loads(data_str)
            for result in data_as_json:
                curr_path = result["id"]
                if result["leaf"]:
                    paths.append(curr_path)
                else:
                    paths = self.fetch_metric_paths(f"{curr_path}.*", paths)
            return sorted(paths)
        except IOError as err:
            raise GraphiteError(f"Failed to fetch metric path from Graphite: {str(err)}")
