Hunter – Hunts Performance Regressions
======================================

_This is an unsupported open source project created by DataStax employees._


Hunter performs statistical analysis of performance test results stored 
in CSV files or Graphite database. It finds change-points and notifies about 
possible performance regressions.  
 
A typical use-case of hunter is as follows: 

- A set of performance tests is scheduled repeatedly.
- The resulting metrics of the test runs are stored in a time series database (Graphite) 
   or appended to CSV files. 
- Hunter is launched by a Jenkins/Cron job (or an operator) to analyze the recorded 
  metrics regularly.
- Hunter notifies about significant changes in recorded metrics by outputting text reports or
  sending Slack notifications.
  
Hunter is capable of finding even small, but systematic shifts in metric values, 
despite noise in data.
It adapts automatically to the level of noise in data and tries not to notify about changes that 
can happen by random. Unlike in threshold-based performance monitoring systems, 
there is no need to setup fixed warning threshold levels manually for each recorded metric.  
The level of accepted probability of false-positives, as well as the 
minimal accepted magnitude of changes are tunable. Hunter is also capable of comparing 
the level of performance recorded in two different periods of time – which is useful for
e.g. validating the performance of the release candidate vs the previous release of your product.    

This is still work-in-progress, unstable code. 
Features may be missing. 
Usability may be unsatisfactory.
Documentation may be incomplete.
Backward compatibility may be broken any time.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development instructions.

## Installation

Hunter requires Python 3.8.  If you don't have python 3.8, 
use pyenv to install it.

Use pipx to install hunter:

```
pipx install git+ssh://git@github.com/datastax-labs/hunter
```

## Setup
Copy the main configuration file `resources/hunter.yaml` to `~/.hunter/hunter.yaml` and adjust 
Graphite and Grafana addresses and credentials. 

Alternatively, it is possible to leave 
the config file as is, and provide credentials in the environment
by setting appropriate environment variables.
Environment variables are interpolated before interpreting the configuration file.

### Defining tests
All test configurations are defined in the main configuration file.
Hunter supports publishing results to a CSV file, [Graphite](https://graphiteapp.org/), and [PostgreSQL](https://www.postgresql.org/).

Tests are defined in the `tests` section.

#### Importing results from CSV
The following definition will import results of the test from a local CSV file: 

```yaml
tests:
  local.sample:
    type: csv
    file: tests/resources/sample.csv
    time_column: time
    metrics: [metric1, metric2]
    attributes: [commit]
    csv_options:
      delimiter: ","
      quote_char: "'"      
```

The `time_column` property points to the name of the column storing the timestamp
of each test-run. The data points will be ordered by that column.

The `metrics` property selects the columns tha hold the values to be analyzed. These values must
be numbers convertible to floats. The `metrics` property can be not only a simple list of column 
names, but it can also be a dictionary configuring other properties of each metric, 
the column name or direction:

```yaml
metrics: 
  resp_time_p99:
    direction: -1
    column: p99
```

Direction can be 1 or -1. If direction is set to 1, this means that the higher the metric, the
better the performance is. If it is set to -1, higher values mean worse performance.

The `attributes` property describes any other columns that should be attached to the final 
report. Special attribute `version` and `commit` can be used to query for a given time-range.


#### Importing results from Graphite

To import data from Graphite, the test configuration must inform Hunter how the
data are published in your history server. This is done by specifying the Graphite path prefix
common for all the test's metrics and suffixes for each of the metrics recorded by the test run.

```yaml
tests:    
  my-product.test:
    type: graphite
    tags: [perf-test, daily, my-product]
    prefix: performance-tests.daily.my-product
    metrics:
      throughput: 
        suffix: client.throughput
      response-time:
        suffix: client.p50
        direction: -1    # lower is better
      cpu-load: 
        suffix: server.cpu
        direction: -1    # lower is better
```

The optional `tags` property contains the tags that are used to query for Graphite events that store 
additional test run metadata such as run identifier, commit, branch and product version information.

The following command will post an event with the test run metadata:
```shell
$ curl -X POST "http://graphite_address/events/" \
    -d '{ 
      "what": "Performance Test", 
      "tags": ["perf-test", "daily", "my-product"],   
      "when": 1537884100,
      "data": {"commit": "fe6583ab", "branch": "new-feature", "version": "0.0.1"}
    }'
```

Posting those events is not mandatory, but when they are available, Hunter is able to 
filter data by commit or version using `--since-commit` or `--since-version` selectors.

#### Importing results from PostgreSQL

To import data from PostgreSQL, Hunter configuration must contain the database connection details:

```yaml
# External systems connectors configuration:
postgres:
  hostname: ...
  port: ...
  username: ...
  password: ...
  database: ...
```

Test configurations must contain a query to select experiment data, a time column, and a list of columns to analyze:

```yaml
tests:
  aggregate_mem:
    type: postgres
    time_column: commit_ts
    attributes: [experiment_id, config_id, commit]
    metrics:
      process_cumulative_rate_mean:
        direction: 1
        scale: 1
      process_cumulative_rate_stderr:
        direction: -1
        scale: 1
      process_cumulative_rate_diff:
        direction: -1
        scale: 1    
    query: |
      SELECT e.commit, 
             e.commit_ts, 
             r.process_cumulative_rate_mean, 
             r.process_cumulative_rate_stderr, 
             r.process_cumulative_rate_diff, 
             r.experiment_id, 
             r.config_id
      FROM results r
      INNER JOIN configs c ON r.config_id = c.id
      INNER JOIN experiments e ON r.experiment_id = e.id
      WHERE e.exclude_from_analysis = false AND
            e.branch = 'trunk' AND
            e.username = 'ci' AND
            c.store = 'MEM' AND
            c.cache = true AND
            c.benchmark = 'aggregate' AND
            c.instance_type = 'ec2i3.large'
      ORDER BY e.commit_ts ASC;
```

For more details, see the examples in [examples/psql](examples/psql).

#### Avoiding test definition duplication
You may find that your test definitions are very similar to each other,
e.g. they all have the same metrics. Instead of copy-pasting the definitions
you can use templating capability built-in hunter to define the common bits of configs separately.

First, extract the common pieces to the `templates` section:
```yaml
templates:
  common-metrics:
    throughput: 
      suffix: client.throughput
    response-time:
      suffix: client.p50
      direction: -1    # lower is better
    cpu-load: 
      suffix: server.cpu
      direction: -1    # lower is better
```

Next you can recall a template in the `inherit` property of the test:

```yaml
my-product.test-1:
  type: graphite
  tags: [perf-test, daily, my-product, test-1]
  prefix: performance-tests.daily.my-product.test-1
  inherit: common-metrics
my-product.test-2:
  type: graphite
  tags: [perf-test, daily, my-product, test-2]
  prefix: performance-tests.daily.my-product.test-2
  inherit: common-metrics
```

You can inherit more than one template.

## Usage
### Listing Available Tests

```
hunter list-groups
hunter list-tests [group name]
```

### Listing Available Metrics for Tests

To list all available metrics defined for the test:
```
hunter list-metrics <test>
```

### Finding Change Points
```
hunter analyze <test>... 
hunter analyze <group>...
```

This command prints interesting results of all
runs of the test and a list of change-points. 
A change-point is a moment when a metric value starts to differ significantly
from the values of the earlier runs and when the difference 
is consistent enough that it is unlikely to happen by chance.  
Hunter calculates the probability (P-value) that the change point was caused 
by chance - the closer to zero, the more "sure" it is about the regression or
performance improvement. The smaller is the actual magnitude of the change,
the more data points are needed to confirm the change, therefore Hunter may
not notice the regression after the first run that regressed.

The `analyze` command accepts multiple tests or test groups.
The results are simply concatenated.

#### Example

```
$ hunter analyze local.sample
INFO: Computing change points for test sample.csv...
sample:
time                         metric1    metric2
-------------------------  ---------  ---------
2021-01-01 02:00:00 +0000     154023      10.43
2021-01-02 02:00:00 +0000     138455      10.23
2021-01-03 02:00:00 +0000     143112      10.29
2021-01-04 02:00:00 +0000     149190      10.91
2021-01-05 02:00:00 +0000     132098      10.34
2021-01-06 02:00:00 +0000     151344      10.69
                                      ·········
                                         -12.9%
                                      ·········
2021-01-07 02:00:00 +0000     155145       9.23
2021-01-08 02:00:00 +0000     148889       9.11
2021-01-09 02:00:00 +0000     149466       9.13
2021-01-10 02:00:00 +0000     148209       9.03
```

### Annotating Change Points in Grafana
Change points found by `analyze` can be exported 
as Grafana annotations using the `--update-grafana` flag:

```
$ hunter analyze <test or group> --update-grafana
```

The annotations generated by Hunter get the following tags:
- `hunter`
- `change-point`  
- `test:<test name>`
- `metric:<metric name>`
- tags configured in the `tags` property of the test
- tags configured in the `annotate` property of the test
- tags configured in the `annotate` property of the metric

Additionally, the `annotate` property supports variable tags:
- `%{TEST_NAME}` - name of the test
- `%{METRIC_NAME}` - name of the metric  
- `%{GRAPHITE_PATH}` - resolves to the path to the data in Graphite
- `%{GRAPHITE_PATH_COMPONENTS}` - splits the path of the data in Graphite into separate components 
  and each path component is exported as a separate tag
- `%{GRAPHITE_PREFIX}` - resolves to the prefix of the path to the data in Graphite 
  (the part of the path up to the metric suffix)
- `%{GRAPHITE_PREFIX_COMPONENTS}` - similar as `%{GRAPHITE_PATH_COMPONENTS}` but splits the prefix
of the path instead of the path
  

### Validating Performance of the Main Branch
Often we want to know if the most recent product version  
performs at least as well as one of the previous releases. It is hard to tell that by looking
at the individual change points. Therefore, Hunter provides a separate command for comparing
the current performance with the baseline performance level denoted by `--since-XXX` selector:

```
$ hunter regressions <test or group> 
$ hunter regressions <test or group> --since <date>
$ hunter regressions <test or group> --since-version <version>
$ hunter regressions <test or group> --since-commit <commit>
```

If there are no regressions found in any of the tests, 
Hunter prints `No regressions found` message. 
Otherwise, it gives a list of tests with metrics and 
magnitude of regressions.
 
In this test, Hunter compares performance level around the baseline ("since") point with 
the performance level at the end of the time series. If the baseline point is not specified, the 
beginning of the time series is assumed. The "performance level at the point" 
is computed from all the data points between two nearest change points. 
Then two such selected fragments are compared using Student's T-test for statistical differences. 

#### Examples
```
$ hunter regressions local.sample
INFO: Computing change points for test local.sample...
local.sample:
    metric2         :     10.5 -->     9.12 ( -12.9%)
Regressions in 1 test found

$ hunter regressions local.sample --since '2021-01-07 02:00:00'
INFO: Computing change points for test local.sample...
local.sample: OK
No regressions found!
```

### Validating Performance of a Feature Branch
The `hunter regressions` command can work with feature branches.

First you need to tell Hunter how to fetch the data of the tests run against a feature branch.
The `prefix` property of the graphite test definition accepts `%{BRANCH}` variable, 
which is substituted at the data import time by the branch name passed to `--branch` 
command argument. Alternatively, if the prefix for the main branch of your product is different
from the prefix used for feature branches, you can define an additional `branch_prefix` property.

```yaml
my-product.test-1:
  type: graphite
  tags: [perf-test, daily, my-product, test-1]
  prefix: performance-tests.daily.%{BRANCH}.my-product.test-1
  inherit: common-metrics

my-product.test-2:
  type: graphite
  tags: [perf-test, daily, my-product, test-2]
  prefix: performance-tests.daily.master.my-product.test-2
  branch_prefix: performance-tests.feature.%{BRANCH}.my-product.test-2
  inherit: common-metrics
```

Now you can verify if correct data are imported by running 
`hunter analyze <test> --branch <branch>`.

The `--branch` argument also works with `hunter regressions`. In this case a comparison will be made
between the tail of the specified branch and the tail of the main branch (or a point of the 
main branch specified by one of the `--since` selectors).

```
$ hunter regressions <test or group> --branch <branch> 
$ hunter regressions <test or group> --branch <branch> --since <date>
$ hunter regressions <test or group> --branch <branch> --since-version <version>
$ hunter regressions <test or group> --branch <branch> --since-commit <commit>
```

Sometimes when working on a feature branch, you may run the tests multiple times,
creating more than one data point. To ignore the previous test results, and compare
only the last few points on the branch with the tail of the main branch, 
use the `--last <n>` selector. E.g. to check regressions on the last run of the tests
on the feature branch:

```
$ hunter regressions <test or group> --branch <branch> --last 1  
```

Please beware that performance validation based on a single data point is quite weak 
and Hunter might miss a regression if the point is not too much different from
the baseline. 

## License

Copyright 2021 DataStax Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
