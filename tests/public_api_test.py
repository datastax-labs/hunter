# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import doctest
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from types import ModuleType

import otava
from otava.series import AnalysisOptions

REPO_ROOT = Path(__file__).parents[1]
DOCS_API = REPO_ROOT / "docs/API.md"

# Where each re-exported name is defined. The package root must hand out the
# very same objects, otherwise isinstance() checks would depend on the import
# path a caller happened to use.
DEFINING_MODULES = {
    "AnalysisOptions": "otava.series",
    "AnalyzedSeries": "otava.series",
    "ChangePoint": "otava.change_point_divisive.base",
    "ChangePointGroup": "otava.change_point_divisive.base",
    "ChangePointsByMetric": "otava.change_point_divisive.base",
    "ChangePointsByTime": "otava.change_point_divisive.base",
    "Metric": "otava.series",
    "Series": "otava.series",
    "compute_change_points": "otava.analysis",
}


def optional_client_modules():
    """Top-level modules the optional service clients import.

    Read off the import_optional_dependency() call sites rather than repeated
    here, so that a new optional client is covered the day it is added.
    """
    call_site = re.compile(r"""import_optional_dependency\(\s*["']([^"']+)["']""")
    names = {
        match.group(1).partition(".")[0]
        for path in (REPO_ROOT / "otava").glob("*.py")
        for match in call_site.finditer(path.read_text(encoding="utf-8"))
    }

    assert names, "no import_optional_dependency() call sites found"
    return sorted(names)


def docs_section(title):
    """The body of one `## ` section of docs/API.md, up to the next one.

    Bounded at both ends on purpose: a regex that ran to the end of the file
    would silently make `title` the only section allowed to come last.
    """
    body = re.search(
        rf"^## {re.escape(title)}\n(.*?)(?=^## |\Z)",
        DOCS_API.read_text(encoding="utf-8"),
        re.M | re.S,
    )

    assert body, f"no '{title}' section in {DOCS_API}"
    return body.group(1)


def documented_public_names():
    return sorted(re.findall(r"^\| `([^`]+)` \|", docs_section("What is public"), re.M))


def test_public_names_are_the_objects_from_their_defining_modules():
    assert set(otava.__all__) == set(DEFINING_MODULES)

    for name, module_name in DEFINING_MODULES.items():
        module = __import__(module_name, fromlist=[name])

        assert getattr(otava, name) is getattr(module, name)


def test_package_root_exports_nothing_beyond_dunder_all():
    attributes = {name: getattr(otava, name) for name in vars(otava)}
    exported = {
        name
        for name, value in attributes.items()
        if not name.startswith("_") and not isinstance(value, ModuleType)
    }

    assert exported == set(otava.__all__)
    assert otava.__all__ == sorted(otava.__all__)

    # Module-valued attributes are the submodules Python binds on import. A
    # module from anywhere else is something the package root pulled in, and the
    # filter above would otherwise hide it. Underscore names are checked too: an
    # `import os as _os` is just as much a leak as `import os`.
    foreign = sorted(
        value.__name__
        for name, value in attributes.items()
        if isinstance(value, ModuleType) and not value.__name__.startswith("otava.")
    )

    assert foreign == []


def test_documented_public_names_match_dunder_all():
    assert documented_public_names() == sorted(otava.__all__)


def test_documented_option_defaults_match_analysis_options():
    rows = re.findall(r"^\| `(\w+)` \| ([^|]+?) \|", docs_section("Options"), re.M)
    defaults = AnalysisOptions()

    assert len(rows) == len(dict(rows)), "an option is documented twice"
    assert dict(rows).keys() == type(defaults).model_fields.keys()
    for option, value in rows:
        assert str(getattr(defaults, option)) == value, option


def test_package_docstring_example_is_accurate():
    results = doctest.testmod(otava, verbose=False)

    assert results.attempted > 0
    assert results.failed == 0


def test_documented_examples_run_and_report_a_change(capsys):
    snippets = re.findall(
        r"^```python\n(.*?)^```", DOCS_API.read_text(encoding="utf-8"), re.M | re.S
    )

    assert snippets, f"no python examples found in {DOCS_API}"

    for snippet in snippets:
        # The page is about the package root. An example that reached into
        # otava.series would still run, and would still document the wrong
        # import, so no submodule path may appear at all.
        assert "from otava import " in snippet
        assert not re.search(r"^\s*(?:from|import) otava\.", snippet, re.M)

        exec(compile(snippet, str(DOCS_API), "exec"), {})

    # An example that finds nothing would still run, and would still be wrong.
    assert capsys.readouterr().out.strip()


def test_importing_the_package_does_not_load_optional_service_clients():
    # The extras introduced in #55 only hold if no code path a core-only
    # installation reaches imports a service client. Importing the package root
    # became such a path when it stopped being an empty namespace package.
    #
    # Poisoning sys.modules rather than builtins.__import__ is deliberate:
    # otava/_optional.py reaches its clients through importlib.import_module(),
    # which does not go through builtins.__import__, so patching that would miss
    # every import written the way this repository writes them.
    script = "blocked = " + repr(optional_client_modules()) + textwrap.dedent(
        """
        import sys

        for name in blocked:
            sys.modules[name] = None

        import otava

        for name in otava.__all__:
            getattr(otava, name)
        """
    )

    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
