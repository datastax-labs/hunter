import enum

from dataclasses import dataclass
from typing import Optional


@dataclass
class CsvOptions:
    delimiter: str
    quote_char: str
    time_column: Optional[str]

    def __init__(self):
        self.delimiter = ","
        self.quote_char = '"'
        self.time_column = None


class CsvColumnType(enum.Enum):
    Numeric = 1
    DateTime = 2
    Str = 3
