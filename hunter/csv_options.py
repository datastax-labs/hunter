import enum
from dataclasses import dataclass


@dataclass
class CsvOptions:
    delimiter: str
    quote_char: str

    def __init__(self):
        self.delimiter = ","
        self.quote_char = '"'


class CsvColumnType(enum.Enum):
    Numeric = 1
    DateTime = 2
    Str = 3
