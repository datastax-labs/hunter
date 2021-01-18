from typing import List

from tabulate import tabulate

from hunter.analysis import PerformanceLog
from hunter.util import format_timestamp, remove_common_prefix, insert_multiple


def column_widths(log: List[str]) -> List[int]:
    return [len(c) for c in log[1].split(None)]


class Report:
    __results: PerformanceLog

    def __init__(self, results: PerformanceLog):
        self.__results = results

    def format_log(self) -> str:
        time_column = [format_timestamp(ts) for ts in self.__results.time]
        table = {"time": time_column, **self.__results.values}
        metrics = list(self.__results.values.keys())
        headers = ["time", *remove_common_prefix(metrics)]
        return tabulate(table, headers=headers)

    def format_log_annotated(self) -> str:
        """Returns test log with change points marked as horizontal lines"""
        change_points = self.__results.find_change_points()
        lines = self.format_log().split("\n")
        col_widths = column_widths(lines)
        indexes = [cp.index for cp in change_points]
        separators = []
        metrics = list(self.__results.values.keys())
        for cp in change_points:
            separator = " " * col_widths[0]
            info = " " * col_widths[0]
            for col_index, col_name in enumerate(metrics):
                col_width = col_widths[col_index + 1]
                change = [c for c in cp.changes if c.metric == col_name]
                if change:
                    change = change[0]
                    change_percent = change.change_percent()
                    separator += "  " + "·" * col_width
                    info += "  " + f"{change_percent:+.1f}%".rjust(col_width)
                else:
                    separator += " " * (col_width + 2)
                    info += " " * (col_width + 2)

            separators.append(f"{separator}\n{info}\n{separator}")

        lines = lines[:2] + insert_multiple(lines[2:], separators, indexes)
        return "\n".join(lines)
