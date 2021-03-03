import enum

from dataclasses import dataclass
from typing import Optional, List


class TestType(enum.Enum):
    Csv = 1
    Fallout = 2


@dataclass
class TestConfig:
    type: TestType
    name: str
    # the following two fields are only used by the FalloutImporter
    user: Optional[str]
    suffixes: Optional[List[str]]

    def __init__(self, name: str, user: Optional[str] = None, suffixes: Optional[List[str]] = None):
        self.name = name
        if self.name.lower().endswith("csv"):
            self.type = TestType.Csv
        else:
            self.type = TestType.Fallout
            self.user = user
            self.suffixes = suffixes
