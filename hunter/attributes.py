from datetime import datetime
from typing import Dict

from hunter.util import format_timestamp


def form_hyperlink_html_str(display_text: str, url: str) -> str:
    return f'<li><a href="{url}" target="_blank">{display_text}</a></li>'


def form_created_msg_html_str() -> str:
    formatted_time = format_timestamp(int(datetime.now().timestamp()), False)
    return f"<p><i><sub>Created by Hunter: {formatted_time}</sub></i></p>"


def get_back_links(attributes: Dict[str, str]) -> str:
    """
    This method is responsible for providing an HTML string corresponding to Fallout and GitHub
    links associated to the attributes of a Fallout-based test run.

    - If no GitHub commit or branch data is provided in the attributes dict, no hyperlink data
    associated to GitHub project repository is provided in the returned HTML.
    """

    # grabbing test runner related data (e.g. Fallout)
    html_str = ""
    if attributes.get("test_url"):
        html_str = form_hyperlink_html_str(display_text="Test", url=attributes.get("test_url"))

    if attributes.get("run_url"):
        html_str = form_hyperlink_html_str(display_text="Test run", url=attributes.get("run_url"))

    # grabbing Github project repository related data
    # TODO: Will we be responsible for handling versioning from repositories aside from bdp?
    repo_url = attributes.get("repo_url", "http://github.com/riptano/bdp")
    if attributes.get("commit"):
        html_str += form_hyperlink_html_str(
            display_text="Git commit", url=f"{repo_url}/commit/{attributes.get('commit')}"
        )
    elif attributes.get("branch"):
        html_str += form_hyperlink_html_str(
            display_text="Git branch", url=f"{repo_url}/tree/{attributes.get('branch')}"
        )
    html_str += form_created_msg_html_str()
    return html_str
