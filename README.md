Hunter – Hunts Performance Regressions
======================================

Hunter fetches performance test results from external systems
like Fallout and Graphite, analyzes them statistically and 
issues reports about possible performance regressions. 

This is work-in-progress, alpha quality software. 
Features may be missing. 
Usability may be unsatisfactory.
Documentation may be incomplete.

## Installation
Hunter requires Python 3.8.
Install the following dependencies:
* [Fallout API](https://github.com/riptano/Fallout/tree/master/fallout-cli)
* [MongoDB Signal Processing Libraries](https://pypi.org/project/signal-processing-algorithms/)
* [Pystache](https://pypi.org/project/pystache/)
* [Ruamel](https://pypi.org/project/ruamel.yaml/)
* [Tabulate](https://pypi.org/project/tabulate/)

## Setup
```
python hunter.py setup
```
The installer will ask you about your Fallout user name and access token
and will save them in `~/.hunter/conf.yaml`. 
You can manually adjust other settings in that file as well. 

You need to be connected to DataStax VPN to allow Hunter to connect
to Fallout and Graphite.

## Usage
### Listing Available Tests
```
python hunter.py list [--user <fallout user>]
``` 

If no user is provided, then user configured in `conf.yaml` is assumed.

### Finding Change Points
```
python hunter.py analyze <fallout test name>
```

This command prints interesting results of all
runs of the test within the last 180 days and a list of change-points. 
A change-point is a moment when a metric value starts to differ significantly
from the values of the earlier runs and when the difference 
is consistent enough that it is unlikely to happen by chance.  
Hunter calculates the probability (P-value) that the change point was caused 
by chance - the closer to zero, the more "sure" it is about the regression or
performance improvement. The smaller is the actual magnitude of the change,
the more data points are needed to confirm the change, therefore Hunter may
not notice the regression after the first run that regressed.

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
 



   