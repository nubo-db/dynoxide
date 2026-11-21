"""The pytest plugin, run against inner test suites in their own processes so
the servers and exported variables stay out of this one."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dynoxide.pytest_plugin import worker_db_path

pytest_plugins = ["pytester"]

SCHEMA = {
    "Table": {
        "TableName": "Things",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
    }
}


@pytest.fixture
def suite(pytester: pytest.Pytester, real_binary: str, monkeypatch: pytest.MonkeyPatch) -> pytest.Pytester:
    monkeypatch.setenv("DYNOXIDE_BINARY", real_binary)
    for name in ("AWS_ENDPOINT_URL_DYNAMODB", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    return pytester


def test_two_tests_share_one_server(suite: pytest.Pytester) -> None:
    suite.makepyfile(
        """
        endpoints = []

        def test_one(dynamodb_client, dynoxide):
            assert dynamodb_client.list_tables()["TableNames"] == []
            endpoints.append(dynoxide.endpoint)

        def test_two(dynamodb_client, dynoxide):
            dynamodb_client.list_tables()
            endpoints.append(dynoxide.endpoint)
            assert len(set(endpoints)) == 1
        """
    )
    suite.runpytest_subprocess().assert_outcomes(passed=2)


def test_a_suite_that_never_asks_starts_nothing(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    # Any attempt to start would fail on this path and error the test.
    monkeypatch.setenv("DYNOXIDE_BINARY", str(pytester.path / "not-a-binary"))
    pytester.makepyfile(
        """
        import os

        def test_plain():
            assert "AWS_ENDPOINT_URL_DYNAMODB" not in os.environ
        """
    )
    pytester.runpytest_subprocess().assert_outcomes(passed=1)


def test_without_boto3_unused_fixtures_are_harmless_and_used_ones_say_why(suite: pytest.Pytester) -> None:
    suite.makeconftest(
        """
        import sys
        sys.modules["boto3"] = None  # import boto3 now raises ImportError
        """
    )
    suite.makepyfile(
        """
        def test_plain():
            pass

        def test_server_alone(dynoxide):
            assert dynoxide.port > 0

        def test_client(dynamodb_client):
            pass
        """
    )
    result = suite.runpytest_subprocess()
    result.assert_outcomes(passed=2, errors=1)
    result.stdout.fnmatch_lines(["*need boto3: pip install boto3*"])


def test_schema_setting_creates_the_tables(suite: pytest.Pytester) -> None:
    (suite.path / "tests").mkdir()
    (suite.path / "tests" / "schema.json").write_text(json.dumps(SCHEMA))
    suite.makepyprojecttoml('[tool.pytest.ini_options]\ndynoxide_schema = "tests/schema.json"\n')
    suite.makepyfile(
        test_tables="""
        def test_first(dynamodb_client):
            assert dynamodb_client.list_tables()["TableNames"] == ["Things"]
        """
    )
    suite.runpytest_subprocess().assert_outcomes(passed=1)


def test_command_line_option_overrides_the_ini_setting(suite: pytest.Pytester) -> None:
    (suite.path / "schema.json").write_text(json.dumps(SCHEMA))
    suite.makepyprojecttoml('[tool.pytest.ini_options]\ndynoxide_schema = "missing.json"\n')
    suite.makepyfile(
        """
        def test_first(dynamodb_client):
            assert dynamodb_client.list_tables()["TableNames"] == ["Things"]
        """
    )
    suite.runpytest_subprocess("--dynoxide-schema", "schema.json").assert_outcomes(passed=1)


def test_dynoxide_clean_empties_tables_and_only_when_asked(suite: pytest.Pytester) -> None:
    (suite.path / "schema.json").write_text(json.dumps(SCHEMA))
    suite.makepyfile(
        """
        def put(client, key):
            client.put_item(TableName="Things", Item={"pk": {"S": key}})

        def count(client):
            return client.scan(TableName="Things", Select="COUNT")["Count"]

        def test_write_and_clean(dynamodb_client, dynoxide_clean):
            for i in range(30):
                put(dynamodb_client, f"item-{i}")

        def test_cleaned(dynamodb_client):
            assert count(dynamodb_client) == 0
            put(dynamodb_client, "kept")

        def test_kept(dynamodb_client):
            assert count(dynamodb_client) == 1
        """
    )
    suite.runpytest_subprocess("--dynoxide-schema", "schema.json", "-p", "no:randomly").assert_outcomes(passed=3)


def test_environment_is_exported_for_the_session_and_restored(suite: pytest.Pytester) -> None:
    suite.makeconftest(
        """
        import os
        os.environ["AWS_ACCESS_KEY_ID"] = "before"
        os.environ["AWS_SESSION_TOKEN"] = "real-token"
        """
    )
    suite.makepyfile(
        """
        import os

        def test_exported(dynoxide):
            assert os.environ["AWS_ENDPOINT_URL_DYNAMODB"] == dynoxide.endpoint
            assert os.environ["AWS_ACCESS_KEY_ID"] == "dynoxide"
            assert "AWS_SESSION_TOKEN" not in os.environ

        def test_default_boto3_client_reaches_it(dynoxide):
            import boto3
            assert boto3.client("dynamodb").list_tables()["TableNames"] == []
        """
    )
    # A session-finish hook in conftest reports the environment once the
    # fixture has been torn down.
    (suite.path / "conftest.py").write_text(
        (suite.path / "conftest.py").read_text()
        + "\n\ndef pytest_unconfigure(config):\n"
        + "    import json, os\n"
        + "    names = ['AWS_ACCESS_KEY_ID', 'AWS_SESSION_TOKEN', 'AWS_ENDPOINT_URL_DYNAMODB']\n"
        + "    with open('after.json', 'w') as f:\n"
        + "        json.dump({n: os.environ.get(n) for n in names}, f)\n"
    )
    suite.runpytest_subprocess().assert_outcomes(passed=2)
    after = json.loads((suite.path / "after.json").read_text())
    assert after == {
        "AWS_ACCESS_KEY_ID": "before",
        "AWS_SESSION_TOKEN": "real-token",
        "AWS_ENDPOINT_URL_DYNAMODB": None,
    }


def test_a_failed_start_errors_every_test_and_is_tried_once(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DYNOXIDE_BINARY", str(pytester.path / "not-a-binary"))
    pytester.makeconftest(
        """
        import dynoxide.pytest_plugin as plugin

        calls = []
        real_start = plugin.start

        def counting(**options):
            calls.append(options)
            return real_start(**options)

        plugin.start = counting

        def pytest_unconfigure(config):
            with open("calls.txt", "w") as f:
                f.write(str(len(calls)))
        """
    )
    pytester.makepyfile(
        """
        def test_one(dynoxide):
            pass

        def test_two(dynoxide):
            pass
        """
    )
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(errors=2)
    result.stdout.fnmatch_lines(["*DynoxideError*not-a-binary*"])
    assert (pytester.path / "calls.txt").read_text() == "1"


def test_each_xdist_worker_gets_its_own_server_and_database(suite: pytest.Pytester) -> None:
    pytest.importorskip("xdist")
    suite.makepyprojecttoml('[tool.pytest.ini_options]\ndynoxide_db_path = "data/test.db"\n')
    (suite.path / "data").mkdir()
    suite.makepyfile(
        **{
            f"test_w{i}": f"""
            import json, os, time

            def test_report(dynoxide):
                time.sleep(0.5)  # long enough that both workers take a test
                worker = os.environ.get("PYTEST_XDIST_WORKER")
                with open(f"report-{{worker}}-{i}.json", "w") as f:
                    json.dump({{"worker": worker, "endpoint": dynoxide.endpoint}}, f)
            """
            for i in range(4)
        }
    )
    suite.runpytest_subprocess("-n", "2").assert_outcomes(passed=4)
    reports = [json.loads(p.read_text()) for p in suite.path.glob("report-*.json")]
    by_worker = {r["worker"]: r["endpoint"] for r in reports}
    assert set(by_worker) == {"gw0", "gw1"}
    assert len(set(by_worker.values())) == 2
    assert sorted(p.name for p in (suite.path / "data").iterdir() if p.name.endswith(("-gw0", "-gw1"))) == [
        "test.db-gw0",
        "test.db-gw1",
    ]


@pytest.mark.parametrize(
    ("worker", "expected"),
    [(None, "data/test.db"), ("", "data/test.db"), ("gw3", "data/test.db-gw3")],
)
def test_worker_db_path(worker: str | None, expected: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    assert worker_db_path("data/test.db", worker) == expected


def test_worker_db_path_reads_the_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw1")
    assert worker_db_path("x.db") == "x.db-gw1"
