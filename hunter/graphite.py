import json
import urllib.request
from dataclasses import dataclass
from logging import info
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


@dataclass
class GraphiteError(IOError):
    message: str


class Graphite:
    __url: str
    __suffixes: List[str]

    def __init__(self, conf: GraphiteConfig):
        self.__url = conf.url
        self.__suffixes = conf.suffixes

    def fetch(self, prefix: str, selector: Optional[str]) -> List[TimeSeries]:
        """
        Connects to Graphite server and downloads interesting series with the
        given prefix. The series to be downloaded are picked from SUFFIXES list.
        """
        try:
            info("Fetching data from Graphite...")
            result = []
            if selector is None:
                selector = "*"
            for suffix in self.__suffixes:
                url = f"{self.__url}render" \
                      f"?target={prefix}.{suffix}.{selector}" \
                      f"&format=json" \
                      f"&from=-365days"
                data_str = urllib.request.urlopen(url).read()
                data_as_json = json.loads(data_str)
                for s in data_as_json:
                    series = TimeSeries(
                        name=s["target"],
                        points=decode_graphite_datapoints(s))
                    result.append(series)

            return result

        except IOError as err:
            raise GraphiteError(
                f"Failed to fetch data from Graphite: {str(err)}")
