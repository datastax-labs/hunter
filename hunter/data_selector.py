import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

import pytz


@dataclass
class DataSelector:
    branch: Optional[str]
    metrics: Optional[List[str]]
    attributes: Optional[List[str]]
    last_n_points: int
    since_commit: Optional[str]
    since_version: Optional[str]
    since_time: datetime
    until_commit: Optional[str]
    until_version: Optional[str]
    until_time: datetime

    def __init__(self):
        self.branch = None
        self.metrics = None
        self.attributes = None
        self.last_n_points = sys.maxsize
        self.since_commit = None
        self.since_version = None
        self.since_time = datetime.now(tz=pytz.UTC) - timedelta(days=365)
        self.until_commit = None
        self.until_version = None
        self.until_time = datetime.now(tz=pytz.UTC)

    def get_selection_description(self):
        attributes = "\n".join(
            [f"{a}: {v}" for a, v in self.__dict__.items() if not a.startswith("__") and v]
        )
        return f"Data Selection\n{attributes}"
