Hunter – Hunts Performance Regressions
======================================

Hunter fetches performance test results from external systems
like Fallout and Graphite, analyzes them statistically and 
issues reports about possible performance regressions. 

This is work-in-progress, alpha quality software. 
Features may be missing. 
Usability may be unsatisfactory.
Documentation may be incomplete.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development instructions.

## Installation

Hunter requires Python 3.8.  If you don't have python 3.8, [use pyenv to install
it](https://datastax.jira.com/wiki/spaces/~741246479/pages/827785323/Coping+with+python+environments).

[Install
pipx](https://datastax.jira.com/wiki/spaces/~741246479/pages/827785323/Coping+with+python+environments),
if you haven't already.

Use pipx to install hunter:

```
pipx install git+ssh://git@github.com/riptano/hunter
```

## Setup
```
hunter setup
```
The installer will ask you about your Fallout user name and access token
and will save them in `~/.hunter/conf.yaml`. 
You can manually adjust other settings in that file as well. 

You need to be connected to DataStax VPN to allow Hunter to connect
to Fallout and Graphite.

## Usage
### Listing Available Tests
```
hunter list [--user <fallout user>]
``` 

If no user is provided, then user configured in `conf.yaml` is assumed.

### Finding Change Points
```
hunter analyze <fallout test name>
hunter analyze <file.csv>
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

#### Example
```
$ hunter analyze tests/resources/sample.csv
INFO: Computing change points...
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

## Limitations
Not all Fallout tests can be analyzed. Hunter works only with tests
that publish their results to Graphite. The test definition 
yaml must contain the following fragment to allow Hunter to locate
the test results in Graphite:

```yaml
ensemble:
  observer:
    configuration_manager:
        - name: ctool_monitoring
          properties:
            graphite.create_server: true
            export.enabled: true
            export.prefix: {{Graphite export prefix}}
            export.metrics: {{Additional metrics}}              
```
