from hunter.fallout import Fallout
from hunter.graphite import Graphite, GraphiteEventData
from typing import Optional, Tuple


class EventProcessor:
    """
    This class provides methods for the processing of metadata stored in Graphite events associated to performance tests,
    and getting appropriate URLs to Fallout tests/test-runs and GitHub branches/commits.
    """
    fallout: Fallout
    graphite: Graphite

    def __init__(self, fallout: Fallout, graphite: Graphite):
        self.fallout = fallout
        self.graphite = graphite

    def __form_hyperlink_html_str(self, url: str, display_text: str) -> str:
        return f'<li><a href="{url}" target="_blank">{display_text}</a></li>'

    def get_html_from_test_run_event(self, test_name, timestamp: int) -> str:
        testrun_event_data = self.graphite.fetch_event_data(self.fallout.get_user(), test_name, timestamp)
        fallout_url_info = self.get_fallout_url_info(testrun_event_data, test_name)
        fallout_url_display_text = fallout_url_info[0]
        fallout_url = fallout_url_info[1]
        html_str = self.__form_hyperlink_html_str(fallout_url, fallout_url_display_text)

        github_url_info = self.get_github_url_info(testrun_event_data)
        if github_url_info is not None:
            github_url_display_text = github_url_info[0]
            github_url = github_url_info[1]
            html_str += self.__form_hyperlink_html_str(github_url, github_url_display_text)
        return html_str

    def get_fallout_url_info(self, testrun_event_data: Optional[GraphiteEventData], test_name: Optional[str]) \
            -> Tuple[str,str]:
        if testrun_event_data is not None:
            return "Fallout test run", self.fallout.get_test_run_url(
                testrun_event_data.test_name,
                testrun_event_data.run_id
            )
        else:
            return "Fallout test", self.fallout.get_test_url(test_name)

    def get_github_url_info(self, testrun_event_data: Optional[GraphiteEventData]) -> Optional[Tuple[str, str]]:
        """
        TODO: Will we be responsible for handling versioning from repositories aside from bdp?
        """
        repo_url = "http://github.com/riptano/bdp"
        if testrun_event_data is not None:
            if testrun_event_data.commit is not None:
                return "Git commit", f"{repo_url}/commit/{testrun_event_data.commit}"
            elif testrun_event_data.branch is not None:
                return "Git branch", f"{repo_url}/tree/{testrun_event_data.branch}"
            else:
                return
        else:
            return
