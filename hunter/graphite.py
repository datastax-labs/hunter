import ast
import json
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from logging import info, warning
from typing import Dict, List, Optional


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


def decode_graphite_datapoints(
        series: Dict[str, List[List[float]]]) -> List[DataPoint]:

    points = series['datapoints']
    return [DataPoint(int(p[1]), p[0])
            for p in points if p[0] is not None]


def to_graphite_time(time: datetime, default: str) -> str:
    if time is not None:
        return time.strftime("%H:%M_%Y%m%d")
    else:
        return default


@dataclass
class GraphiteError(IOError):
    message: str


@dataclass
class DataSelector:
    metrics: Optional[List[str]]
    from_time: Optional[datetime]
    until_time: Optional[datetime]

    def __init__(self):
        self.metrics = None
        self.from_time = None
        self.until_time = None


@dataclass
class GraphiteEventData:
    test_owner: str
    test_name: str
    run_id: str
    status: str
    start_time: int
    end_time: int
    version: Optional[str]
    branch: Optional[str]
    commit: Optional[str]

    def __init__(self,
                 test_owner: str,
                 test_name: str,
                 run_id: str,
                 status: str,
                 start_time: str,
                 end_time: str,
                 version: Optional[str],
                 branch: Optional[str],
                 commit: Optional[str]):
        self.test_owner = test_owner
        self.test_name = test_name
        self.run_id = run_id,
        self.status = status,
        self.start_time = start_time
        self.end_time = end_time
        if len(version) == 0 or version == 'null':
            self.version = None
        else:
            self.version = version
        if len(branch) == 0 or branch == 'null':
            self.branch = None
        else:
            self.branch = branch
        if len(commit) == 0 or commit == 'null':
            self.commit = None
        else:
            self.commit = commit


class Graphite:
    __url: str
    __suffixes: List[str]

    def __init__(self, conf: GraphiteConfig):
        self.__url = conf.url
        self.__suffixes = conf.suffixes

    def fetch_event_data(self, fallout_user: str, test_name: str, timestamp: int) -> Optional[GraphiteEventData]:
        """
        Queries the Graphite events API endpoint, and filters down the returned events which have data that matches all
        of the following criteria:
        - the Fallout user of interest
        - the Fallout test of interest
        - start and end timestamps for Fallout run which passed in metric timestamp is bounded between

        References:
            - Graphite events REST API: https://graphite.readthedocs.io/en/stable/events.html
            - Haxx: https://github.com/riptano/haxx/pull/588
        """
        try:
            url = f"{self.__url}events/get_data"
            data_str = urllib.request.urlopen(url).read()
            data_as_json = json.loads(data_str)
            performance_test_events = filter(lambda event: event.get("what") == "Performance Test", data_as_json)
            performance_test_events_data = map(lambda event: ast.literal_eval(event.get("data")), performance_test_events)
            candidate_events_data = list(filter(lambda event_data: event_data["test_name"] == test_name and
                                                          event_data["test_owner"] == fallout_user and
                                                          event_data["start_time"] <= timestamp
                                                          <= event_data["end_time"], performance_test_events_data))
            test_event_data = None
            if len(candidate_events_data) > 1:
                warning(f"Found multiple potential Graphite events for {fallout_user}'s test {test_name} "
                        f"with metric timestamp {candidate_events_data}")
            elif len(candidate_events_data) == 1:
                test_event_data = GraphiteEventData(**candidate_events_data[0])
            else:
                warning(f"No Graphite events for {fallout_user}'s test {test_name} with metric timestamp {timestamp}")
            return test_event_data
        except IOError as e:
            raise GraphiteError(f"Failed to fetch Graphite events: {str(e)}")

    def fetch(self, prefix: str, selector: DataSelector) \
            -> List[TimeSeries]:
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
            from_time = to_graphite_time(selector.from_time, "-365d")
            until_time = to_graphite_time(selector.until_time, "now")
            for suffix in self.__suffixes:
                url = f"{self.__url}render" \
                      f"?target={prefix}.{suffix}{metrics}" \
                      f"&format=json" \
                      f"&from={from_time}" \
                      f"&until={until_time}"
                data_str = urllib.request.urlopen(url).read()
                data_as_json = json.loads(data_str)
                for s in data_as_json:
                    series = TimeSeries(
                        name=s["target"],
                        points=decode_graphite_datapoints(s))
                    if len(series.points) > 5:
                        result.append(series)
                    else:
                        warning(
                            f"Not enough data points in series {series.name}. "
                            f"Required at least 5 points.")

            return result

        except IOError as err:
            raise GraphiteError(
                f"Failed to fetch data from Graphite: {str(err)}")
