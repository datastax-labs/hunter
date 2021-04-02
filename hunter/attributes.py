from hunter.fallout import Fallout
from typing import Dict, Optional


def form_hyperlink_html_str(display_text: str, url: str) -> str:
    return f'<li><a href="{url}" target="_blank">{display_text}</a></li>'


def get_html_from_attributes(
    test_name: str, user: Optional[str], attributes: Optional[Dict[str, str]], fallout: Fallout
) -> str:
    """
    This method is responsible for providing an HTML string corresponding to Fallout and GitHub
    links associated to the attributes of a Fallout-based test run.

    - If no GitHub commit or branch data is provided in the attributes dict, no hyperlink data
    associated to GitHub project repository is provided in the returned HTML.
    """
    if attributes is not None:
        # grabbing Fallout test related data
        if attributes.get("run"):
            html_str = form_hyperlink_html_str(
                display_text="Fallout test run",
                url=fallout.get_test_run_url(test_name, user, attributes.get("run")),
            )
        else:
            html_str = form_hyperlink_html_str(
                display_text="Fallout test", url=fallout.get_test_url(test_name, user)
            )

        # grabbing Github project repository related data
        # TODO: Will we be responsible for handling versioning from repositories aside from bdp?
        repo_url = "http://github.com/riptano/bdp"
        if attributes.get("commit"):
            html_str += form_hyperlink_html_str(
                display_text="Git commit", url=f"{repo_url}/commit/{attributes.get('commit')}"
            )
        elif attributes.get("branch"):
            html_str += form_hyperlink_html_str(
                display_text="Git branch", url=f"{repo_url}/tree/{attributes.get('branch')}"
            )
    else:
        html_str = form_hyperlink_html_str(
            display_text="Fallout test", url=fallout.get_test_url(test_name)
        )
    return html_str
