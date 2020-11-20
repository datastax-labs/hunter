import itertools
from dataclasses import dataclass
from typing import Optional, List

import pystache
from fallout.api import FalloutAPI
from requests.exceptions import HTTPError
from ruamel.yaml import YAML


@dataclass
class FalloutConfig:
    user: str
    token: str
    url: str


def test_params(definition: str) -> str:
    """
    Returns the lines until the first line equal to '---'.
    If there is no '---', an empty string is returned.
    """
    lines = definition.splitlines()
    if "---" in lines:
        lines = itertools.takewhile(lambda l: l != "---", lines)
        return "\n".join(lines)
    else:
        return ""


def test_template(definition: str) -> str:
    """
    Returns the lines after the first line equal to '---'
    If there is no '---', returns the original definition.
    """
    lines = definition.splitlines()
    if "---" in lines:
        lines = itertools.dropwhile(lambda l: l != "---", lines)
        next(lines)  # skip "---"
        return "\n".join(lines)
    else:
        return definition


def test_yaml(definition: str) -> dict:
    """Substitutes test parameters and returns parsed test definition yaml"""
    yaml = YAML(typ='safe')
    params = test_params(definition)
    params = yaml.load(params)
    template = test_template(definition)
    final_yaml = pystache.render(template, params)
    return yaml.load(final_yaml)


@dataclass
class FalloutTest:
    name: str
    definition: dict

    def graphite_prefix(self) -> Optional[str]:
        """
        Returns the Graphite database prefix where metrics of this test
        are exported. If the test definition doesn't contain the
        `export.prefix` property, returns None.
        This prefix is needed to construct a query to Graphite to fetch
        performance results of the test runs.
        """
        try:
            cfg_manager = self.definition["ensemble"]["observer"][
                "configuration_manager"]
            ctool_monitoring = next(
                x for x in cfg_manager if x['name'] == 'ctool_monitoring')
            properties = ctool_monitoring['properties']
            return properties['export.prefix']

        except KeyError:
            return None


@dataclass
class FalloutError(IOError):
    message: str


class Fallout:
    __api: FalloutAPI
    __user: str

    def __init__(self, conf: FalloutConfig):
        try:
            self.__user = conf.user
            self.__api = FalloutAPI(conf.url, conf.token)
            self.__api.validate_server_version()
        except IOError as err:
            raise FalloutError(f"Failed to communicate with Fallout: {str(err)}")

    def get_test(self, test_name: str, user: Optional[str] = None) \
            -> FalloutTest:
        """"Returns YAML of the Fallout test with the given name"""
        try:
            if user is None:
                user = self.__user
            test = self.__api.test_info(test_name, user)
            definition = test["definition"]
            return FalloutTest(test_name, test_yaml(definition))

        except HTTPError as err:
            if err.response.status_code == 404:
                raise FalloutError(f"Test not found: {test_name}")
            else:
                raise FalloutError(f"Failed to fetch test {test_name}: "
                                   f"{str(err)}")

    def list_tests(self, user: Optional[str]) -> List[str]:
        """"Returns the list of available Fallout tests"""
        if user is None:
            user = self.__user
        return [t['name'] for t in self.__api.list_tests(user)]

