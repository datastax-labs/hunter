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

"""Change detection for continuous performance engineering.

The names re-exported here are the public API of Otava as a library: the types
needed to build a Series from data Otava did not import itself, analyze it, and
read the change points back.

    >>> from otava import AnalysisOptions, Metric, Series
    >>> series = Series(
    ...     "throughput",
    ...     branch=None,
    ...     time=list(range(40)),
    ...     metrics={"ops": Metric(direction=1, scale=1.0)},
    ...     data={"ops": [100.0] * 20 + [80.0] * 20},
    ...     attributes={},
    ... )
    >>> analyzed = series.analyze(AnalysisOptions(window_len=20))
    >>> [group.time for group in analyzed.change_points_by_time]
    [20]

Anything reached through a submodule path, such as ``otava.importer`` or
``otava.main``, is internal to the command-line tool and may change without
notice.
"""

from otava.analysis import compute_change_points
from otava.change_point_divisive.base import (
    ChangePoint,
    ChangePointGroup,
    ChangePointsByMetric,
    ChangePointsByTime,
)
from otava.series import AnalysisOptions, AnalyzedSeries, Metric, Series

__all__ = [
    "AnalysisOptions",
    "AnalyzedSeries",
    "ChangePoint",
    "ChangePointGroup",
    "ChangePointsByMetric",
    "ChangePointsByTime",
    "Metric",
    "Series",
    "compute_change_points",
]
