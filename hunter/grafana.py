import re
import requests

from dataclasses import asdict, dataclass
from logging import info
from requests.exceptions import HTTPError
from typing import Dict, List, Optional


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
    dashboard_id: int
    panel_id: int
    time: int
    text: str
    tags: List[str]


class PanelMetric:
    delimiter: str
    target_query: str
    valid_template_variables: List[str]
    valid_metric: bool
    parametrized_metric: str
    regex_pattern: str

    def __init__(self, target_query: str, valid_template_variables: List[str]):
        self.delimiter = "."
        self.target_query = target_query
        self.valid_template_variables = valid_template_variables
        self.valid_metric = True
        self.__extract_parametrized_metric()
        self.__create_regex_pattern()

    def __extract_parametrized_metric(self):
        """
        A Grafana target query may involve the composition of several Grafana functions one after
        the next to a provided (possibly parametrized) metric (+ other arguments at each function
        application). A relatively extreme example of such:

        averageSeriesWithWildcards(
            aliasSub(
                movingAverage(
                    performance_regressions.$frequency.$component.$test.$workload.$environment.*.
                    if_octets.rx, 5
                ),
                '.*\..*\.(.*)\..*\..*\..*-rps([^\.]*)\..*', '\1\2'
            ), 3
        )

        To extract out the (parametrized) metric from the target query, we try:
        - restrict to everything before first occurrence of a whitespace
        - remove any trailing comma (i.e. case that metric + additional arguments passed to some
        innermost Grafana function)
        - remove trailing closing parentheses (i.e. case that metric was only argument passed to
        some innermost Grafana function)
        - get everything after last occurrence of opening parentheses

        NOTE: This may not handle all cases properly, and will probably be subject to further
        refining/improvements.
        """
        self.parametrized_metric = self.target_query.split()[0].strip(",").strip(")").split("(")[-1]

    def __create_regex_pattern(self):
        delimiter_pattern = f"\\{self.delimiter}"
        beginning_of_string = "^"
        end_of_string = "$"
        regex_pattern = beginning_of_string

        tokens = self.parametrized_metric.split(self.delimiter)
        for token in tokens:
            """replacing any template variable, wildcards, or option in the graphite query,
            and the trailing period we split along"""
            token = self.__replace_wildcards(token)
            token = self.__replace_template_variables(token)
            token = self.__replace_or_options(token)
            regex_pattern += f"{token}"
            regex_pattern += delimiter_pattern
        # strip trailing delimiter_pattern, and specify end of string
        regex_pattern = regex_pattern.strip(delimiter_pattern) + end_of_string
        self.regex_pattern = regex_pattern

    def __replace_template_variables(self, metric_token: str) -> str:
        """
        Grafana metric queries can use template variables (denoted by $variable_name) as a token
        in the path, e.g.

            performance_regressions.$frequency.$component.$test.$workload.$environment.*.
            disk_ops.read

        This method will replace a templated variable with the regex pattern [^.]+ in a Grafana
        metric query token.
        """
        template_variable_index = metric_token.find("$")
        if template_variable_index > -1:
            template_variable = metric_token[template_variable_index + 1 :]
            if template_variable not in self.valid_template_variables:
                self.valid_metric = False
            new_metric_token = f"{metric_token[:template_variable_index]}[^{self.delimiter}]+"
            return new_metric_token
        else:
            new_metric_token = metric_token
        return new_metric_token

    def __replace_wildcards(self, metric_token: str) -> str:
        """
        Grafana metric queries can use wildcards either as an entire token in the path, e.g.

            performance_regressions.$frequency.$component.$test.$workload.$environment.*.
            disk_ops.read

        or as part of a token, e.g.

            performance_regressions.$frequency.$component.$test.$workload.$environment.server_*.
            disk_ops.read

        This helper method will recursively convert all wildcards with the regex pattern [^.]+ in
        a Grafana metric query token.
        """
        wildcard_index = metric_token.find("*")
        if wildcard_index > -1:
            new_metric_token = (
                f"{metric_token[:wildcard_index]}"
                f"[^{self.delimiter}]+"
                f"{metric_token[wildcard_index + 1:]}"
            )
            return self.__replace_wildcards(new_metric_token)
        else:
            return metric_token

    def __replace_or_options(self, metric_token: str) -> str:
        """
        Grafana parametrized metric queries can contain a hard-coded set of options somewhere in the
        path. For instance:

            performance_regressions.$frequency.$component.$test.$workload.$environment.*.
            GC-{ParNew,G1_Young_Generation}.gauge-CollectionCount

        contains {ParNew,G1_Young_Generation} hared-coded options.

        This helper method searches for this pattern in {option_1, ..., option_N} in a Grafana
        metric query token, and converts appropriately to regex pattern (option_1|...|option_N).
        """
        or_start_index = metric_token.find("{")
        if or_start_index > -1:
            or_end_index = metric_token.find("}")
            options_string = metric_token[or_start_index + 1 : or_end_index]
            options = [option.strip() for option in options_string.split(",")]
            new_metric_token = f"{metric_token[:or_start_index]}("
            for option in options:
                new_metric_token += f"{option}|"
            new_metric_token = new_metric_token.strip("|")
            new_metric_token += f"){metric_token[or_end_index + 1:]}"
            return self.__replace_or_options(new_metric_token)
        else:
            return metric_token

    def determine_tags(self, hardcoded_metric: str) -> List[str]:
        """
        Takes in a hard-coded metric path (i.e. one without any wildcards or variables), and
        determines all of the tokens in this query that correspond to parametrized variables or
        wildcards. These will constitute the set of tags that would let us identify which metric
        (displayed in a particular panel) an annotations corresponds to.

        Note that in the case that this class' parametrized_query does not have any variables or
        wildcards, this will simply return back an empty list.
        """
        hardcoded_metric_query_tokens = hardcoded_metric.split(self.delimiter)
        parametrized_metric_tokens = self.parametrized_metric.split(self.delimiter)
        assert len(hardcoded_metric_query_tokens) == len(
            parametrized_metric_tokens
        ), f"Query pattern mismatch: {hardcoded_metric} vs. {self.parametrized_metric}"
        tags = []
        for index, token in enumerate(hardcoded_metric_query_tokens):
            if token != parametrized_metric_tokens[index]:
                tags.append(token)
        return tags


class Panel:
    __id: int
    __title: str
    __parent_dashboard_template_variables = List[str]
    __panel_metrics: Dict[str, PanelMetric]

    def __init__(self, panel_info: dict, parent_dashboard_template_variables: List[str]):
        self.__id = panel_info["id"]
        self.__title = panel_info["title"]
        self.__parent_dashboard_template_variables = parent_dashboard_template_variables
        self.__initialize_panel_metrics(panel_info["targets"])

    def __initialize_panel_metrics(self, targets_info: List[Dict[str, str]]):
        self.__panel_metrics = {}
        for target_info in targets_info:
            target_query = target_info.get("target")
            if target_query is not None and len(target_query) > 0:
                self.__panel_metrics[target_query] = PanelMetric(
                    target_query=target_query,
                    valid_template_variables=self.__parent_dashboard_template_variables,
                )

    def get_title(self):
        return self.__title

    def get_panel_metrics(self) -> Dict[str, PanelMetric]:
        return self.__panel_metrics


class Dashboard:
    __uid: str
    __id: int
    __title: str
    __template_variables = List[str]
    __panels: Dict[int, Panel]

    def __init__(self, dashboard_info: dict):
        self.__uid = dashboard_info["uid"]
        self.__id = dashboard_info["id"]
        self.__title = dashboard_info["title"]
        self.__template_variables = [
            variable.get("name") for variable in dashboard_info["templating"].get("list")
        ]
        self.__initialize_panels(dashboard_info)

    def __initialize_panels(self, dashboard_info: dict):
        rows_info = dashboard_info.get("rows")
        panels_info = []
        """
        the case that the dashboard contains rows, extract panels from each row and 
        concatenate into single list
        """
        if rows_info is not None:
            for row in rows_info:
                panels_info += row.get("panels")
        else:
            panels_info = dashboard_info.get("panels")
        # get any panels that correspond to a graph
        graph_panels_info = list(filter(lambda p: p["type"] == "graph", panels_info))
        rows = filter(lambda p: p["type"] == "row", panels_info)
        for row in rows:
            row_panels_info = row["panels"]
            for panel_info in row_panels_info:
                if panel_info.get("type") == "graph":
                    graph_panels_info.append(panel_info)
        self.__panels = {}
        for panel_info in graph_panels_info:
            panel_id = panel_info["id"]
            self.__panels[panel_id] = Panel(panel_info, self.__template_variables)

    def get_title(self):
        return self.__title

    def get_panels(self) -> Dict[int, Panel]:
        return self.__panels

    def get_panel(self, panel_id) -> Panel:
        assert panel_id in self.__panels, f"Invalid panel id: {panel_id}"
        return self.__panels[panel_id]


class Grafana:
    __url: str
    __user: str
    __password: str
    __dashboards: Dict[int, Dashboard]

    def __init__(self, grafana_conf: GrafanaConfig):
        self.__url = grafana_conf.url
        self.__user = grafana_conf.user
        self.__password = grafana_conf.password
        self.__initialize_dashboards()

    def __initialize_dashboards(self):
        """
        Reference:
         - https://grafana.com/docs/grafana/latest/http_api/folder_dashboard_search/
         - https://grafana.com/docs/grafana/latest/http_api/dashboard/#get-dashboard-by-uid
        """
        info("Fetching Grafana dashboards information...")
        dashboards_dict = {}
        url = f"{self.__url}api/search"
        try:
            response = requests.get(url, auth=(self.__user, self.__password))
            response.raise_for_status()
        except HTTPError as err:
            raise GrafanaError(str(err))
        # restrict to only those JSON objects corresponding to a dashboard
        dashboards_metadata = filter(lambda d: d.get("type") == "dash-db", response.json())
        for dashboard_metadata in dashboards_metadata:
            dashboard_uid = dashboard_metadata["uid"]
            dashboard_id = dashboard_metadata["id"]
            dashboard_url = f"{self.__url}api/dashboards/uid/{dashboard_uid}"
            try:
                response = requests.get(dashboard_url, auth=(self.__user, self.__password))
                response.raise_for_status()
                dashboard_info = response.json()["dashboard"]
                dashboards_dict[dashboard_id] = Dashboard(dashboard_info=dashboard_info)
            except HTTPError as err:
                raise GrafanaError(str(err))
        self.__dashboards = dashboards_dict

    def get_dashboards(self) -> Dict[int, Dashboard]:
        return self.__dashboards

    def get_dashboard(self, dashboard_id: int) -> Dashboard:
        assert dashboard_id in self.__dashboards, f"Invalid dashboard id: {dashboard_id}"
        return self.__dashboards[dashboard_id]

    def find_all_dashboard_panels_displaying(self, metric: str) -> List[Dict]:
        results = []
        dashboards = self.get_dashboards()
        for dashboard_id, dashboard in dashboards.items():
            panels = dashboard.get_panels()
            for panel_id, panel in panels.items():
                panel_metrics = panel.get_panel_metrics()
                for panel_metric in panel_metrics.values():
                    if panel_metric.valid_metric:
                        regex_pattern = panel_metric.regex_pattern
                        if re.match(regex_pattern, metric):
                            tags = panel_metric.determine_tags(metric)
                            results.append(
                                {"dashboard id": dashboard_id, "panel id": panel_id, "tags": tags}
                            )
        return results

    def fetch_matching_annotations(
        self,
        annotation: Optional[Annotation] = None,
        dashboard_id: Optional[int] = None,
        panel_id: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Reference:
        - https://grafana.com/docs/grafana/latest/http_api/annotations/#find-annotations
        """
        if annotation is not None:
            return self.fetch_matching_annotations(
                dashboard_id=annotation.dashboard_id,
                panel_id=annotation.panel_id,
                tags=annotation.tags,
            )
        else:
            url = f"{self.__url}api/annotations"
            query_parameters = {}
            if dashboard_id is not None:
                query_parameters["dashboardId"] = dashboard_id
            if panel_id is not None:
                query_parameters["panelId"] = panel_id
            if tags is not None:
                query_parameters["tags"] = tags
            try:
                response = requests.get(
                    url=url, params=query_parameters, auth=(self.__user, self.__password)
                )
                response.raise_for_status()
                return response.json()
            except HTTPError as err:
                raise GrafanaError(str(err))

    def delete_matching_annotations(
        self,
        annotation: Optional[Annotation] = None,
        dashboard_id: Optional[int] = None,
        panel_id: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ):
        """
        Reference:
        - https://grafana.com/docs/grafana/latest/http_api/annotations/#delete-annotation-by-id
        """
        if annotation is not None:
            matching_annotation_dicts = self.fetch_matching_annotations(annotation=annotation)
        else:
            matching_annotation_dicts = self.fetch_matching_annotations(
                dashboard_id=dashboard_id, panel_id=panel_id, tags=tags
            )
        url = f"{self.__url}api/annotations"
        for annotation_dict in matching_annotation_dicts:
            annotation_id = annotation_dict["id"]
            annotation_url = f"{url}/{annotation_id}"
            try:
                response = requests.delete(url=annotation_url, auth=(self.__user, self.__password))
                response.raise_for_status()
            except HTTPError as err:
                raise GrafanaError(str(err))

    def post_annotation(self, annotation: Annotation):
        """
        Reference:
        - https://grafana.com/docs/grafana/latest/http_api/annotations/#create-annotation
        """
        dashboard_title = self.__dashboards[annotation.dashboard_id].get_title()
        panel_title = (
            self.__dashboards[annotation.dashboard_id].get_panel(annotation.panel_id).get_title()
        )
        info(f"Creating annotation for dashboard: {dashboard_title}, panel: {panel_title}...")
        url = f"{self.__url}api/annotations"
        try:
            response = requests.post(
                url=url, data=asdict(annotation), auth=(self.__user, self.__password)
            )
            response.raise_for_status()
        except HTTPError as err:
            raise GrafanaError(str(err))
