from hunter.graphite import compress_target_paths


def test_compress_target_paths():
    paths = [
        "foo.bar.p50",
        "foo.bar.p75",
        "foo.bar.p99",
        "foo.foo.baz.p50",
        "foo.foo.baz.p75",
        "foo.foo.baz.throughput",
        "something.else",
    ]

    assert set(compress_target_paths(paths)) == {
        "foo.bar.{p50,p75,p99}",
        "foo.foo.baz.{p50,p75,throughput}",
        "something.else",
    }
