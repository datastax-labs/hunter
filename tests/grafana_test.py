import re

from typing import List
from hunter.grafana import PanelMetric


def assert_on_matching_metric_paths(
    panel_metric: PanelMetric, hardcoded_metric_path: str, expected_tags: List[str]
):
    """
    Makes sure that the generated regex pattern for some parametrized Grafana metric query
    (this information is stored in the panel_metric object that is passed in) matches up completely
    with a hardcoded/non-parametrized Grafana metric query. Then makes sure that the list of
    determined annotation tags matches up with an expected list of tags.
    """
    match = re.match(panel_metric.regex_pattern, hardcoded_metric_path)
    assert match is not None, (
        f"Did not match {hardcoded_metric_path} to generated regex "
        f"pattern {panel_metric.regex_pattern} for "
        f"parametrized metric {panel_metric.parametrized_metric}"
    )
    assert (
        match.start() == 0
    ), f"Start of regex match was at character {match.start()} of {hardcoded_metric_path}"
    assert match.end() == len(hardcoded_metric_path), (
        f"End of regex match was at early character {match.end()} " f"of {hardcoded_metric_path}"
    )
    determined_tags = panel_metric.determine_tags(hardcoded_metric_path)
    assert determined_tags == expected_tags, (
        f"Parametrized metric: {panel_metric.parametrized_metric}, "
        f"Hardcoded metric: {hardcoded_metric_path}, "
        f"Determined tags: {determined_tags}, "
        f"Expected tags: {expected_tags}"
    )


def assert_on_non_matching_metric_paths(panel_metric: PanelMetric, hardcoded_metric_path: str):
    """
    Makes sure that the generated regex pattern for some parametrized Grafana metric query
    (this information is stored in the panel_metric object that is passed in) does NOT match up
    with a hardcoded/non-parametrized Grafana metric query.
    """
    match = re.match(panel_metric.regex_pattern, hardcoded_metric_path)
    assert match is None, (
        f"Got invalid regex match of {hardcoded_metric_path} with regex pattern "
        f"{panel_metric.regex_pattern} for parametrized metric {panel_metric.parametrized_metric}"
    )


def test_panel_metric_is_valid():
    parametrized_metric_path = "foo.bar.$node.metric"
    valid_template_variables = ["node"]
    panel_metric = PanelMetric(
        target_query=parametrized_metric_path, valid_template_variables=valid_template_variables
    )
    assert panel_metric.valid_metric == True, (
        f"Incorrectly interpreted {parametrized_metric_path} as invalid for valid "
        f"template variables {valid_template_variables}"
    )


def test_panel_metric_is_invalid():
    parametrized_metric_path = "foo.bar.$node.metric"
    valid_template_variables = ["foo"]
    panel_metric = PanelMetric(
        target_query=parametrized_metric_path, valid_template_variables=valid_template_variables
    )
    assert panel_metric.valid_metric == False, (
        f"Incorrectly interpreted {parametrized_metric_path} as valid, for valid "
        f"template variables {valid_template_variables}"
    )


def test_replace_single_template_variable_token():
    parametrized_metric_path = "foo.bar.$node.metric"
    panel_metric = PanelMetric(
        target_query=parametrized_metric_path, valid_template_variables=["node"]
    )
    matching_metric_path_to_tags_dict = {"foo.bar.server_node0.metric": ["server_node0"]}
    non_matching_metric_paths = [
        "foo.bar.metric",  # not enough tokens in path
        "foo.bar.server_node0",  # not enough tokens in path
        "foo.bar.server_node0.other_metric",  # `other_metric` should not match with `metric`
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)


def test_replace_multiple_template_variable_tokens():
    parametrized_metric_path = "$foo.bar.$node.metric"
    panel_metric = PanelMetric(
        target_query=parametrized_metric_path, valid_template_variables=["foo", "node"]
    )
    matching_metric_path_to_tags_dict = {"foo.bar.server_node0.metric": ["foo", "server_node0"]}
    non_matching_metric_paths = [
        "bar.metric",  # not enough tokens in path
        "foo.bar.metric",  # not enough tokens in path
        "bar.server_node0.node",  # not enough tokens in path
        "foo.bar.server_node0.other_metric",  # `other_metric` should not match `metric`
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)


def test_replace_single_wildcard_token():
    parametrized_metric_path = "foo.bar.*.metric"
    panel_metric = PanelMetric(target_query=parametrized_metric_path, valid_template_variables=[])
    matching_metric_path_to_tags_dict = {"foo.bar.server_node1.metric": ["server_node1"]}
    non_matching_metric_paths = [
        "foo.bar.metric",  # not enough tokens in path
        "foo.bar.server_node0",  # not enough tokens in path
        "foo.bar.server_node1.other_metric",  # `other_metric` should not match with `metric`
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)


def test_replace_multiple_wildcard_tokens():
    parametrized_metric_path = "foo.*.server_node0.*"
    panel_metric = PanelMetric(target_query=parametrized_metric_path, valid_template_variables=[])
    matching_metric_path_to_tags_dict = {"foo.bar.server_node0.metric": ["bar", "metric"]}
    non_matching_metric_paths = [
        "foo.bar.server_node0",  # not enough tokens in path
        "foo.server_node0.metric",  # not enough tokens in path
        "foo.server_node0",  # not enough tokens in path
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)


def test_replace_multiple_wildcards_in_single_token():
    parametrized_metric_path = "foo.bar.*_node*.metric"
    panel_metric = PanelMetric(target_query=parametrized_metric_path, valid_template_variables=[])
    matching_metric_path_to_tags_dict = {
        "foo.bar.server_node0.metric": ["server_node0"],
        "foo.bar.server_node1.metric": ["server_node1"],
        "foo.bar.client_node0.metric": ["client_node0"],
        "foo.bar.client_node1.metric": ["client_node1"],
    }
    non_matching_metric_paths = [
        "foo.bar.metric",  # not enough tokens in path
        "foo.bar.server_node0",  # not enough tokens in path
        "foo.bar._node.metric",  # `_node` should not match with `*_node*`
        "foo.bar.server_node.metric",  # `server_node` should not match with `*_node*`
        "foo.bar.node0.metric",  # `node0` should not match with `*_node*`
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)


def test_replace_single_or_option_token():
    parametrized_metric_path = "foo.bar.{client,server}_node0.metric"
    panel_metric = PanelMetric(target_query=parametrized_metric_path, valid_template_variables=[])
    matching_metric_path_to_tags_dict = {
        "foo.bar.client_node0.metric": ["client_node0"],
        "foo.bar.server_node0.metric": ["server_node0"],
    }
    non_matching_metric_paths = [
        "foo.bar.metric",  # not enough tokens in path
        "foo.bar.client_node0",  # not enough tokens in path
        "foo.bar._node0.metric",  # options {client,server} should not match
        "foo.bar.observer_node0.metric",  # options {client,server} should not match
        "foo.bar.client_node1.metric",  # `client_node1` should not match `{client,server}_node0`
        "foo.bar.server_node1.metric",  # `server_node1` should not match `{client,server}_node0`
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)


def test_replace_multiple_or_option_tokens():
    parametrized_metric_path = "foo.bar.{client,server}_node0.{metric_A,metric_B}"
    panel_metric = PanelMetric(target_query=parametrized_metric_path, valid_template_variables=[])
    matching_metric_path_to_tags_dict = {
        "foo.bar.server_node0.metric_A": ["server_node0", "metric_A"],
        "foo.bar.server_node0.metric_B": ["server_node0", "metric_B"],
        "foo.bar.client_node0.metric_A": ["client_node0", "metric_A"],
        "foo.bar.client_node0.metric_B": ["client_node0", "metric_B"],
    }
    non_matching_metric_paths = [
        "foo.bar.client_node0",  # not enough tokens in path
        "foo.bar.metric_A",  # not enough tokens in path
        "foo.bar._node0.metric_A",  # options {client,server} should not match
        "foo.bar.observer_node0.metric_A",  # options {client,server} should not match
        "foo.bar.server_node0.metric_C",  # options {metric_A,metric_B} should not match
        "foo.bar.client_node0.metric_C",  # options {metric_A,metric_B} should not match
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)


def test_replace_multiple_or_options_in_single_token():
    parametrized_metric_path = "foo.bar.{client,server}_node{0,1}.metric"
    panel_metric = PanelMetric(target_query=parametrized_metric_path, valid_template_variables=[])
    matching_metric_path_to_tags_dict = {
        "foo.bar.client_node0.metric": ["client_node0"],
        "foo.bar.client_node1.metric": ["client_node1"],
        "foo.bar.server_node0.metric": ["server_node0"],
        "foo.bar.server_node1.metric": ["server_node1"],
    }
    non_matching_metric_paths = [
        "foo.bar.metric",  # not enough tokens in path
        "foo.bar.client_node0",  # not enough tokens in path
        "foo.bar._node.metric",  # {client,server} or {0,1} options in token should not match
        "foo.bar._node0.metric",  # first {client,server} options in token should not match
        "foo.bar.observer_node0.metric",  # first {client,server} options in token should not match
        "foo.bar.client_node.metric",  # second {0,1} options in token should not match
        "foo.bar.client_node3.metric",  # second {0,1} options in token should not match
    ]
    for path, tags in matching_metric_path_to_tags_dict.items():
        assert_on_matching_metric_paths(panel_metric, path, tags)
    for path in non_matching_metric_paths:
        assert_on_non_matching_metric_paths(panel_metric, path)
