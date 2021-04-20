Hunter – Hunts Performance Regressions
======================================

Hunter fetches performance test results from external systems
like Fallout and Graphite, analyzes them statistically and 
issues reports about possible performance regressions. 

This is work-in-progress, alpha quality software. 
Features may be missing. 
Usability may be unsatisfactory.
Documentation may be incomplete.
Backward compatibility may be broken any time.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development instructions.

## Installation

Hunter requires Python 3.8.  If you don't have python 3.8, 
[use pyenv to install it](https://datastax.jira.com/wiki/spaces/~741246479/pages/827785323/Coping+with+python+environments).

[Install
pipx](https://datastax.jira.com/wiki/spaces/~741246479/pages/827785323/Coping+with+python+environments),
if you haven't already.

Use pipx to install hunter:

```
pipx install git+ssh://git@github.com/riptano/hunter
```

## Setup
Copy the main configuration file `resources/hunter.yaml` to `~/.hunter/hunter.yaml` and adjust 
Graphite and Grafana addresses and credentials. 

Alternatively, it is possible to leave 
the config file as is, and provide credentials in the environment
by setting appropriate environment variables.
Environment variables are expanded in the config file properties.

### Defining tests
All test configurations are defined in the main configuration file.
Currently, there are two types of tests supported: tests that publish
their results to a CSV file, and tests that publish their results
to a Graphite database.

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
The optional `tags` property contains the tags that are used to query for Graphite events that store 
additional test run metadata such as run identifier, commit, branch and product version information.

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

### Example

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
