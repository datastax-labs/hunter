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

import csv
import os
import subprocess
import sys
import tempfile
from importlib import metadata
from pathlib import Path

from otava.bigquery import BigQuery, BigQueryConfig
from otava.grafana import Grafana, GrafanaConfig
from otava.influxdb import InfluxDB, InfluxDBConfig
from otava.postgres import Postgres, PostgresConfig
from otava.slack import _create_slack_notifier

OPTIONAL_DISTRIBUTIONS = {
    "google-cloud-bigquery": "bigquery",
    "pg8000": "postgres",
    "influxdb3-python": "influxdb",
    "requests": "grafana",
    "slack-sdk": "slack",
}


def assert_optional_distributions_are_absent():
    for distribution in OPTIONAL_DISTRIBUTIONS:
        try:
            metadata.version(distribution)
        except metadata.PackageNotFoundError:
            continue
        raise AssertionError(f"{distribution} was installed by the default package")


def assert_public_api_is_installed():
    # A wheel that drops otava/__init__.py leaves `import otava` resolving to an
    # empty namespace package again, which no unit test can see: they run from
    # the source tree, where the file is always present.
    import otava

    if otava.__file__ is None:
        raise AssertionError("otava/__init__.py was not installed with the default package")

    missing = [name for name in otava.__all__ if not hasattr(otava, name)]
    if missing:
        raise AssertionError(f"public names missing from the installed package: {missing}")

    strong, _weak = otava.compute_change_points([10.0] * 20 + [30.0] * 20, window_len=10)
    if [point.index for point in strong] != [20]:
        raise AssertionError("compute_change_points found no change through the package root")


def assert_cli_help_works():
    result = subprocess.run(
        [sys.executable, "-m", "otava.main", "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout


def assert_csv_analysis_works():
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        data_dir = root / "data"
        data_dir.mkdir()
        with (data_dir / "sample.csv").open("w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["time", "metric"])
            for day, value in enumerate([10, 11, 9, 10, 30, 31, 29, 30], start=1):
                writer.writerow([f"2026-01-{day:02d}T00:00:00+00:00", value])

        config = root / "otava.yaml"
        config.write_text(
            """tests:
  local.sample:
    type: csv
    file: data/sample.csv
    time_column: time
    metrics: [metric]
""",
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, "-m", "otava.main", "analyze", "local.sample"],
            cwd=root,
            capture_output=True,
            text=True,
            env={**os.environ, "OTAVA_CONFIG": str(config)},
        )
        assert result.returncode == 0, result.stderr
        assert "metric" in result.stdout


def assert_missing_extra(extra_name, operation):
    try:
        operation()
    except ModuleNotFoundError as err:
        expected = f"pip install 'apache-otava[{extra_name}]'"
        assert expected in str(err), str(err)
        return
    raise AssertionError(f"{extra_name} operation did not report its missing extra")


def assert_optional_operations_name_their_extras():
    operations = {
        "bigquery": lambda: BigQuery(
            BigQueryConfig("project", "dataset", "credentials.json")
        ).client,
        "postgres": lambda: Postgres(
            PostgresConfig("localhost", 5432, "user", "password", "database")
        ).fetch_data("SELECT 1"),
        "influxdb": lambda: InfluxDB(
            InfluxDBConfig("http://localhost:8181", "database", "token")
        ).client,
        "grafana": lambda: Grafana(
            GrafanaConfig("https://example.invalid/", "user", "password")
        ).fetch_annotations(None, None),
        "slack": lambda: _create_slack_notifier("token"),
    }
    for extra_name, operation in operations.items():
        assert_missing_extra(extra_name, operation)


def main():
    assert_optional_distributions_are_absent()
    assert_public_api_is_installed()
    assert_cli_help_works()
    assert_csv_analysis_works()
    assert_optional_operations_name_their_extras()


if __name__ == "__main__":
    main()
