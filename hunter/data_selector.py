from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime, timedelta

import pytz


@dataclass
class DataSelector:
    metrics: Optional[List[str]]
    attributes: Optional[List[str]]
    since_commit: Optional[str]
    since_version: Optional[str]
    since_time: datetime
    until_commit: Optional[str]
    until_version: Optional[str]
    until_time: datetime

    def __init__(self):
        self.metrics = None
        self.attributes = None
        self.since_commit = None
        self.since_version = None
        self.since_time = datetime.now(tz=pytz.UTC) - timedelta(days=365)
        self.until_commit = None
        self.until_version = None
        self.until_time = datetime.now(tz=pytz.UTC)
