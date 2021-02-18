from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

import pytz

@dataclass
class DataSelector:
    """
    TODO: Is it worth the extra level of abstraction to make this a parent class for CsvDataSelector and
    GraphiteDataSelector classes (and eventually PrometheusDataSelector)?
    """
    suffixes: Optional[List[str]]
    metrics: Optional[List[str]]
    attributes: Optional[List[str]]
    from_time: datetime
    until_time: datetime

    def __init__(self):
        self.suffixes = None
        self.metrics = None
        self.attributes = None
        self.from_time = datetime.fromtimestamp(0, tz=pytz.UTC)
        self.until_time = datetime.now(tz=pytz.UTC)
