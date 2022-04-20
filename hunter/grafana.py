from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Optional

import requests
from pytz import UTC
from requests.exceptions import HTTPError


@dataclass
class GrafanaConfig:
    url: str
    user: str
    password: str


@dataclass
class GrafanaError(Exception):
    message: str


@dataclass
class Annotation:
    id: Optional[int]
    time: datetime
    text: str
    tags: List[str]


class Grafana:
    url: str
    __user: str
    __password: str

    def __init__(self, grafana_conf: GrafanaConfig):
        self.url = grafana_conf.url
        self.__user = grafana_conf.user
        self.__password = grafana_conf.password

    def fetch_annotations(
        self, start: Optional[datetime], end: Optional[datetime], tags: List[str] = None
    ) -> List[Annotation]:
        """
        Reference:
        - https://grafana.com/docs/grafana/latest/http_api/annotations/#find-annotations
        """
        url = f"{self.url}api/annotations"
        query_parameters = {}
        if start is not None:
            query_parameters["from"] = int(start.timestamp() * 1000)
        if end is not None:
            query_parameters["to"] = int(end.timestamp() * 1000)
        if tags is not None:
            query_parameters["tags"] = tags
        try:
            response = requests.get(
                url=url, params=query_parameters, auth=(self.__user, self.__password)
            )
            response.raise_for_status()
            json = response.json()
            annotations = []
            for annotation_json in json:
                annotation = Annotation(
                    id=annotation_json["id"],
                    time=datetime.fromtimestamp(float(annotation_json["time"]) / 1000, tz=UTC),
                    text=annotation_json["text"],
                    tags=annotation_json["tags"],
                )
                annotations.append(annotation)

            return annotations

        except KeyError as err:
            raise GrafanaError(f"Missing field {err.args[0]}")
        except HTTPError as err:
            raise GrafanaError(str(err))

    def delete_annotations(self, *ids: int):
        """
        Reference:
        - https://grafana.com/docs/grafana/latest/http_api/annotations/#delete-annotation-by-id
        """
        url = f"{self.url}api/annotations"
        for annotation_id in ids:
            annotation_url = f"{url}/{annotation_id}"
            try:
                response = requests.delete(url=annotation_url, auth=(self.__user, self.__password))
                response.raise_for_status()
            except HTTPError as err:
                raise GrafanaError(str(err))

    def create_annotations(self, *annotations: Annotation):
        """
        Reference:
        - https://grafana.com/docs/grafana/latest/http_api/annotations/#create-annotation
        """
        try:
            url = f"{self.url}api/annotations"
            for annotation in annotations:
                data = asdict(annotation)
                data["time"] = int(annotation.time.timestamp() * 1000)
                del data["id"]
                response = requests.post(url=url, data=data, auth=(self.__user, self.__password))
                response.raise_for_status()
        except HTTPError as err:
            raise GrafanaError(str(err))
