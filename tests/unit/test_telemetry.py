import pytest

from lakehouse.ops import telemetry


@pytest.fixture
def ops_down(monkeypatch):
    def boom():
        raise ConnectionError("postgres unreachable password=hunter2")

    monkeypatch.setattr(telemetry, "connect", boom)


def test_instrumented_is_fail_open(ops_down):
    @telemetry.instrumented(source="s", entity="e")
    def job():
        return {"rows_read": 1}

    assert job() == {"rows_read": 1}


def test_instrumented_reraises_job_errors(ops_down):
    @telemetry.instrumented()
    def job():
        raise ValueError("bad")

    with pytest.raises(ValueError):
        job()


def test_watermarks_are_not_fail_open(ops_down):
    with pytest.raises(ConnectionError):
        telemetry.get_watermark("s", "e")
