from collections import OrderedDict
from enum import Enum, unique
from typing import List

from tabulate import tabulate

from hunter.series import ChangePointGroup, Series
from hunter.util import format_timestamp, insert_multiple, remove_common_prefix


@unique
class ReportType(Enum):
    LOG = "log"
    JSON = "json"
    REGRESSIONS_ONLY = "regressions_only"

    def __str__(self):
        return self.value


class Report:
    __series: Series
    __change_points: List[ChangePointGroup]

    def __init__(self, series: Series, change_points: List[ChangePointGroup]):
        self.__series = series
        self.__change_points = change_points

    @staticmethod
    def __column_widths(log: List[str]) -> List[int]:
        return [len(c) for c in log[1].split(None)]

    def produce_report(self, test_name: str, report_type: ReportType):
        if report_type == ReportType.LOG:
            return self.__format_log_annotated(test_name)
        elif report_type == ReportType.JSON:
            return self.__format_json(test_name)
        elif report_type == ReportType.REGRESSIONS_ONLY:
            return self.__format_regressions_only(test_name)
        else:
            from hunter.main import HunterError

            raise HunterError(f"Unknown report type: {report_type}")

    def __format_log(self) -> str:
        time_column = [format_timestamp(ts) for ts in self.__series.time]
        table = {"time": time_column, **self.__series.attributes, **self.__series.data}
        metrics = list(self.__series.data.keys())
        headers = list(
            OrderedDict.fromkeys(
                ["time", *self.__series.attributes, *remove_common_prefix(metrics)]
            )
        )
        return tabulate(table, headers=headers)

    def __format_log_annotated(self, test_name: str) -> str:
        """Returns test log with change points marked as horizontal lines"""
        lines = self.__format_log().split("\n")
        col_widths = self.__column_widths(lines)
        indexes = [cp.index for cp in self.__change_points]
        separators = []
        columns = list(
            OrderedDict.fromkeys(["time", *self.__series.attributes, *self.__series.data])
        )
        for cp in self.__change_points:
            separator = ""
            info = ""
            for col_index, col_name in enumerate(columns):
                col_width = col_widths[col_index]
                change = [c for c in cp.changes if c.metric == col_name]
                if change:
                    change = change[0]
                    change_percent = change.forward_change_percent()
                    separator += "·" * col_width + "  "
                    info += f"{change_percent:+.1f}%".rjust(col_width) + "  "
                else:
                    separator += " " * (col_width + 2)
                    info += " " * (col_width + 2)

            separators.append(f"{separator}\n{info}\n{separator}")

        lines = lines[:2] + insert_multiple(lines[2:], separators, indexes)
        return "\n".join(lines)

    def __format_json(self, test_name: str) -> str:
        import json

        return json.dumps({test_name: [cpg.to_json() for cpg in self.__change_points]})

    def __format_regressions_only(self, test_name: str) -> str:
        output = []
        for cpg in self.__change_points:
            regressions = []
            for cp in cpg.changes:
                metric = self.__series.metrics[cp.metric]
                if metric.direction * cp.forward_change_percent() < 0:
                    regressions.append(
                        (
                            cp.metric,
                            cp.stats.mean_1,
                            cp.stats.mean_2,
                            cp.stats.forward_rel_change() * 100.0,
                        )
                    )

            if regressions:
                output.append(format_timestamp(cpg.time))
                output.extend(
                    [
                        "    {:16}:\t{:#8.3g}\t--> {:#8.3g}\t({:+6.1f}%)".format(*args)
                        for args in regressions
                    ]
                )

        if output:
            return f"Regressions in {test_name}:" + "\n" + "\n".join(output)
        else:
            return f"No regressions found in {test_name}."
