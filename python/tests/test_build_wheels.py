"""The wheel builder, against small archives made here whose binaries have
just enough of a header to pass or fail its checks."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import io
import struct
import sys
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_wheels.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("build_wheels", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["build_wheels"] = module
    spec.loader.exec_module(module)
    return module


bw = _load()


def elf(machine: int, interp: bool = False) -> bytes:
    header = bytearray(64)
    header[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", header, 18, machine)
    struct.pack_into("<Q", header, 32, 64)  # program headers right after
    struct.pack_into("<HH", header, 54, 56, 1)
    program = bytearray(56)
    struct.pack_into("<I", program, 0, 3 if interp else 1)  # PT_INTERP or PT_LOAD
    return bytes(header + program)


def macho(cputype: int, major: int, minor: int, old_style: bool = False) -> bytes:
    header = struct.pack("<IIIIIIII", 0xFEEDFACF, cputype, 0, 2, 1, 24, 0, 0)
    version = (major << 16) | (minor << 8)
    if old_style:
        command = struct.pack("<IIII", 0x24, 16, version, 0)
    else:
        command = struct.pack("<IIIIII", 0x32, 24, 1, version, 0, 0)
    return header + command


def pe(machine: int) -> bytes:
    data = bytearray(0x80)
    data[:2] = b"MZ"
    struct.pack_into("<I", data, 0x3C, 0x40)
    data[0x40:0x44] = b"PE\0\0"
    struct.pack_into("<H", data, 0x44, machine)
    return bytes(data)


GOOD = {
    "x86_64-unknown-linux-musl": elf(62),
    "aarch64-unknown-linux-musl": elf(183),
    "x86_64-apple-darwin": macho(0x01000007, 10, 12, old_style=True),
    "aarch64-apple-darwin": macho(0x0100000C, 11, 0),
    "x86_64-pc-windows-msvc": pe(0x8664),
}


def tar_gz(path: Path, members: list[tuple[tarfile.TarInfo, bytes | None]]) -> None:
    with tarfile.open(path, "w:gz") as tf:
        for info, data in members:
            tf.addfile(info, io.BytesIO(data) if data is not None else None)


def regular(name: str, data: bytes) -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = 0o755
    return info, data


def write_assets(directory: Path, binaries: dict[str, bytes] | None = None) -> Path:
    directory.mkdir(exist_ok=True)
    for target in bw.TARGETS:
        data = (binaries or GOOD)[target.triple]
        archive = directory / target.archive
        if target.kind == "pe":
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr(target.member, data)
        else:
            tar_gz(archive, [regular(target.member, data)])
    sums = "".join(
        f"{hashlib.sha256((directory / t.archive).read_bytes()).hexdigest()}  {t.archive}\n" for t in bw.TARGETS
    )
    (directory / "sha256sums.txt").write_text(sums)
    return directory


@pytest.fixture
def pure(tmp_path: Path) -> Path:
    path = tmp_path / "dynoxide-1.8.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("dynoxide/__init__.py", "")
        zf.writestr("dynoxide-1.8.0.dist-info/METADATA", "Name: dynoxide\nVersion: 1.8.0\n")
        zf.writestr(
            "dynoxide-1.8.0.dist-info/WHEEL",
            "Wheel-Version: 1.0\nGenerator: hatchling\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        zf.writestr("dynoxide-1.8.0.dist-info/RECORD", "")
    return path


def _sums_line(directory: Path, archive: str) -> None:
    lines = [
        line
        if not line.endswith(archive)
        else f"{hashlib.sha256((directory / archive).read_bytes()).hexdigest()}  {archive}"
        for line in (directory / "sha256sums.txt").read_text().splitlines()
    ]
    (directory / "sha256sums.txt").write_text("\n".join(lines) + "\n")


def test_five_wheels_with_their_tags_binary_and_record(pure: Path, tmp_path: Path) -> None:
    out = tmp_path / "out"
    wheels = bw.build(pure, write_assets(tmp_path / "assets"), out)
    assert sorted(w.name for w in wheels) == sorted(
        [
            "dynoxide-1.8.0-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.musllinux_1_1_x86_64.whl",
            "dynoxide-1.8.0-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.musllinux_1_1_aarch64.whl",
            "dynoxide-1.8.0-py3-none-macosx_10_12_x86_64.whl",
            "dynoxide-1.8.0-py3-none-macosx_11_0_arm64.whl",
            "dynoxide-1.8.0-py3-none-win_amd64.whl",
        ]
    )
    for wheel, target in zip(wheels, bw.TARGETS, strict=True):
        with zipfile.ZipFile(wheel) as zf:
            binaries = [n for n in zf.namelist() if n.startswith("dynoxide/_bin/")]
            assert binaries == [f"dynoxide/_bin/{target.member}"]
            info = zf.getinfo(binaries[0])
            assert (info.external_attr >> 16) == 0o100755
            assert zf.read(info) == GOOD[target.triple]

            wheel_file = zf.read("dynoxide-1.8.0.dist-info/WHEEL").decode()
            assert "Root-Is-Purelib: false" in wheel_file
            assert "Root-Is-Purelib: true" not in wheel_file
            tags = [line.split(": ")[1] for line in wheel_file.splitlines() if line.startswith("Tag:")]
            assert tags == [f"py3-none-{t}" for t in target.platform_tag.split(".")]

            record = zf.read("dynoxide-1.8.0.dist-info/RECORD").decode().splitlines()
            assert "dynoxide-1.8.0.dist-info/RECORD,," in record
            listed = {line.split(",")[0] for line in record}
            assert listed == set(zf.namelist())
            for line in record:
                name, digest, size = line.split(",")
                if name.endswith("RECORD"):
                    continue
                data = zf.read(name)
                expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                assert digest == f"sha256={expected}"
                assert int(size) == len(data)


def test_a_hash_mismatch_writes_no_wheel(pure: Path, tmp_path: Path) -> None:
    assets = write_assets(tmp_path / "assets")
    target = bw.TARGETS[-1]
    with zipfile.ZipFile(assets / target.archive, "a") as zf:
        zf.comment = b"changed"
    out = tmp_path / "out"
    with pytest.raises(bw.BuildError, match="SHA-256"):
        bw.build(pure, assets, out)
    assert not out.exists() or not list(out.iterdir())


def test_an_archive_missing_from_the_sums_is_refused(pure: Path, tmp_path: Path) -> None:
    assets = write_assets(tmp_path / "assets")
    lines = (assets / "sha256sums.txt").read_text().splitlines()[1:]
    (assets / "sha256sums.txt").write_text("\n".join(lines) + "\n")
    with pytest.raises(bw.BuildError, match="no line"):
        bw.build(pure, assets, tmp_path / "out")


@pytest.mark.parametrize(
    "members",
    [
        [regular("dynoxide", b"x"), regular("README", b"x")],
        [regular("../dynoxide", b"x")],
        [regular("bin/dynoxide", b"x")],
        "symlink",
        "hardlink",
    ],
)
def test_archives_with_anything_but_the_binary_are_refused(
    pure: Path, tmp_path: Path, members: list[tuple[tarfile.TarInfo, bytes]] | str
) -> None:
    assets = write_assets(tmp_path / "assets")
    target = bw.TARGETS[0]
    if isinstance(members, str):
        link = tarfile.TarInfo("dynoxide")
        link.type = tarfile.SYMTYPE if members == "symlink" else tarfile.LNKTYPE
        link.linkname = "/bin/sh"
        tar_gz(assets / target.archive, [(link, None)])
    else:
        tar_gz(assets / target.archive, list(members))
    _sums_line(assets, target.archive)
    with pytest.raises(bw.BuildError):
        bw.build(pure, assets, tmp_path / "out")


def test_a_zip_with_a_symlink_is_refused(pure: Path, tmp_path: Path) -> None:
    assets = write_assets(tmp_path / "assets")
    target = bw.TARGETS[-1]
    with zipfile.ZipFile(assets / target.archive, "w") as zf:
        info = zipfile.ZipInfo("dynoxide.exe")
        info.external_attr = 0o120777 << 16
        zf.writestr(info, "target")
    _sums_line(assets, target.archive)
    with pytest.raises(bw.BuildError, match="regular file"):
        bw.build(pure, assets, tmp_path / "out")


@pytest.mark.parametrize(
    ("triple", "binary", "message"),
    [
        ("x86_64-unknown-linux-musl", elf(62, interp=True), "dynamically linked"),
        ("x86_64-unknown-linux-musl", elf(183), "machine"),
        ("aarch64-apple-darwin", macho(0x0100000C, 13, 0), "needs macOS 13.0"),
        ("x86_64-apple-darwin", macho(0x0100000C, 10, 12), "CPU type"),
        ("x86_64-pc-windows-msvc", pe(0xAA64), "PE machine"),
        ("x86_64-pc-windows-msvc", elf(62), "Windows executable"),
    ],
)
def test_a_binary_for_the_wrong_platform_is_refused(
    pure: Path, tmp_path: Path, triple: str, binary: bytes, message: str
) -> None:
    assets = write_assets(tmp_path / "assets", {**GOOD, triple: binary})
    with pytest.raises(bw.BuildError, match=message):
        bw.build(pure, assets, tmp_path / "out")


def test_only_a_pure_wheel_is_accepted(tmp_path: Path, pure: Path) -> None:
    platform_wheel = tmp_path / "dynoxide-1.8.0-py3-none-win_amd64.whl"
    platform_wheel.write_bytes(pure.read_bytes())
    with pytest.raises(bw.BuildError, match="pure"):
        bw.build(platform_wheel, write_assets(tmp_path / "assets"), tmp_path / "out")


def test_command_line_reports_failure(tmp_path: Path, pure: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert bw.main(["--pure", str(pure), "--assets", str(tmp_path / "none"), "--out", str(tmp_path / "o")]) == 1
    assert "sha256sums.txt is missing" in capsys.readouterr().err
