from hunter.analysis import PerformanceLog


def test_change_point_detection():

    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    log = PerformanceLog("test", time, {"series1": series1, "series2": series2})

    change_points = log.find_change_points()
    assert len(change_points) == 2
    assert change_points[0].index == 4
    assert change_points[0].changes[0].metric == "series2"
    assert change_points[1].index == 6
    assert change_points[1].changes[0].metric == "series1"