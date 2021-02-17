from collections import OrderedDict
from typing import List

from tabulate import tabulate

from hunter.analysis import PerformanceLog
from hunter.util import format_timestamp, insert_multiple, remove_common_prefix


def column_widths(log: List[str]) -> List[int]:
    return [len(c) for c in log[1].split(None)]


class Report:
    __results: PerformanceLog

    def __init__(self, results: PerformanceLog):
        self.__results = results

    def format_log(self) -> str:
        time_column = [format_timestamp(ts) for ts in self.__results.time]
        table = {"time": time_column,
                 **self.__results.attributes,
                 **self.__results.data}
        metrics = list(self.__results.data.keys())
        headers = list(OrderedDict.fromkeys(
            ["time", *self.__results.attributes, *remove_common_prefix(metrics)]))
        return tabulate(table, headers=headers)

    def format_log_annotated(self) -> str:
        """Returns test log with change points marked as horizontal lines"""
        change_points = self.__results.find_change_points()
        lines = self.format_log().split("\n")
        col_widths = column_widths(lines)
        indexes = [cp.index for cp in change_points]
        separators = []
        columns = list(OrderedDict.fromkeys(
            ["time", *self.__results.attributes, *self.__results.data]))
        for cp in change_points:
            separator = ""
            info = ""
            for col_index, col_name in enumerate(columns):
                col_width = col_widths[col_index]
                change = [c for c in cp.changes if c.metric == col_name]
                if change:
                    change = change[0]
                    change_percent = change.change_percent()
                    separator += "·" * col_width + "  "
                    info += f"{change_percent:+.1f}%".rjust(col_width) + "  "
                else:
                    separator += " " * (col_width + 2)
                    info += " " * (col_width + 2)

            separators.append(f"{separator}\n{info}\n{separator}")

        lines = lines[:2] + insert_multiple(lines[2:], separators, indexes)
        return "\n".join(lines)
