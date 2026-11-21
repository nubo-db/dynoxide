from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import pytest
from conftest import FAKE

import dynoxide
from dynoxide import DynoxideError, _binary
from dynoxide._binary import Binary, checked, child_env, parse_version, resolve


@pytest.fixture
def bundled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    path = tmp_path / "bundled" / "dynoxide"
    path.parent.mkdir()
    path.write_text("")
    monkeypatch.setattr(_binary, "binary_path", lambda: str(path))
    return str(path)


@pytest.fixture
def other(tmp_path: Path) -> str:
    path = tmp_path / "other-dynoxide"
    path.write_text("")
    return str(path)


def test_binary_argument_wins_over_the_environment(bundled: str, other: str, tmp_path: Path) -> None:
    from_env = tmp_path / "from-env"
    from_env.write_text("")
    found = resolve(other, False, {"DYNOXIDE_BINARY": str(from_env)})
    assert (found.path, found.source) == (other, "option")


def test_environment_wins_over_the_bundled_binary(bundled: str, other: str) -> None:
    found = resolve(None, False, {"DYNOXIDE_BINARY": other})
    assert (found.path, found.source) == (other, "env")


def test_bundled_binary_is_the_default(bundled: str) -> None:
    found = resolve(None, False, {})
    assert (found.path, found.source) == (bundled, "package")


def test_empty_environment_variable_counts_as_unset(bundled: str) -> None:
    assert resolve(None, False, {"DYNOXIDE_BINARY": ""}).source == "package"


def test_relative_binary_is_made_absolute(other: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(Path(other).parent)
    found = resolve(Path(other).name, False, {})
    assert found.path == other


def test_no_bundled_binary_names_the_platform_and_the_variable() -> None:
    # The editable install has no binary, as an install from the sdist has none.
    with pytest.raises(DynoxideError) as caught:
        resolve(None, False, {})
    assert caught.value.code == "UNSUPPORTED_PLATFORM"
    assert "DYNOXIDE_BINARY" in str(caught.value)
    assert _binary._platform_name() in str(caught.value)
    with pytest.raises(DynoxideError):
        dynoxide.binary_path()


def test_missing_binary_argument_names_the_path(tmp_path: Path) -> None:
    missing = tmp_path / "nope" / "dynoxide"
    with pytest.raises(DynoxideError) as caught:
        resolve(missing, False, {})
    assert caught.value.code == "BINARY_MISSING"
    assert str(missing) in str(caught.value)


def test_command_name_gets_a_hint_about_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(DynoxideError, match="binary_from_path"):
        resolve("dynoxide", False, {})


def test_missing_environment_binary_is_an_error_not_a_fallback(bundled: str, tmp_path: Path) -> None:
    with pytest.raises(DynoxideError) as caught:
        resolve(None, False, {"DYNOXIDE_BINARY": str(tmp_path / "gone")})
    assert caught.value.code == "BINARY_MISSING"
    assert "DYNOXIDE_BINARY" in str(caught.value)


def test_binary_argument_of_the_wrong_type() -> None:
    with pytest.raises(TypeError):
        resolve(42, False, {})  # type: ignore[arg-type]


def _executable(path: Path, text: str) -> Path:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.mark.skipif(sys.platform == "win32", reason="PATH lookup of a shell script")
def test_binary_from_path_skips_this_packages_own_command(tmp_path: Path) -> None:
    shim_dir = tmp_path / "venv-bin"
    real_dir = tmp_path / "homebrew-bin"
    shim_dir.mkdir()
    real_dir.mkdir()
    _executable(shim_dir / "dynoxide", "#!/usr/bin/env python\nfrom dynoxide.__main__ import main\nmain()\n")
    real = _executable(real_dir / "dynoxide", "#!/bin/sh\necho dynoxide 1.8.0\n")
    found = resolve(None, True, {"PATH": os.pathsep.join([str(shim_dir), str(real_dir)])})
    assert (found.path, found.source) == (str(real), "path")


def test_binary_from_path_with_nothing_on_path(tmp_path: Path) -> None:
    with pytest.raises(DynoxideError) as caught:
        resolve(None, True, {"PATH": str(tmp_path)})
    assert caught.value.code == "BINARY_MISSING"


def _check(version: str | None = None, mode: str = "ready") -> Binary:
    env = {**os.environ, "FAKE_MODE": mode}
    if version:
        env["FAKE_VERSION"] = version
    return checked(Binary(str(FAKE), "option"), [sys.executable], env)


def test_current_version_gets_the_launcher_flags() -> None:
    found = _check("1.8.0")
    assert (found.version, found.launcher_flags) == ("1.8.0", True)


def test_pre_release_counts_as_its_release() -> None:
    found = _check("1.7.0-rc.1")
    assert (found.version, found.launcher_flags) == ("1.7.0-rc.1", True)


def test_1_6_is_started_the_legacy_way() -> None:
    found = _check("1.6.3")
    assert (found.version, found.launcher_flags) == ("1.6.3", False)


def test_older_than_1_6_is_refused_naming_both_versions() -> None:
    with pytest.raises(DynoxideError) as caught:
        _check("1.5.0")
    assert caught.value.code == "TOO_OLD"
    assert "1.5.0" in str(caught.value) and "1.6.0" in str(caught.value)


@pytest.mark.parametrize("mode", ["version-garbage", "version-fail"])
def test_unreadable_version(mode: str) -> None:
    with pytest.raises(DynoxideError) as caught:
        _check(mode=mode)
    assert caught.value.code == "VERSION_CHECK_FAILED"


def test_the_bundled_binary_is_not_asked_its_version() -> None:
    found = Binary("/nowhere/dynoxide", "package")
    assert checked(found) is found


@pytest.mark.parametrize(
    ("output", "version"),
    [
        ("dynoxide 1.8.0\n", "1.8.0"),
        ("dynoxide 1.9.0-rc.1\r\n", "1.9.0-rc.1"),
        ("\ndynoxide 1.8.0+build.5\n", "1.8.0+build.5"),
        ("dynoxide 1.8\n", None),
        ("something 1.8.0\n", None),
        ("", None),
    ],
)
def test_parse_version(output: str, version: str | None) -> None:
    assert parse_version(output) == version


def test_child_environment_drops_the_encryption_key() -> None:
    env = child_env({"DYNOXIDE_ENCRYPTION_KEY": "secret", "KEEP": "1"})
    assert env == {"KEEP": "1"}
