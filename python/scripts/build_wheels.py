#!/usr/bin/env python3
"""Builds the platform wheels from a pure wheel and a release's assets.

    python scripts/build_wheels.py --pure dist/dynoxide-1.8.0-py3-none-any.whl \\
        --assets release-assets/ --out wheelhouse/

The pure wheel is the package code, built once by hatchling. For each release
target this checks the archive against sha256sums.txt, takes the one binary
out of it, checks the binary is for that platform (and for Linux that it's
static, which the musllinux and manylinux tags together depend on), and writes
a wheel with the binary at dynoxide/_bin/ and that platform's tags. Every
wheel carries the same code; only the binary and the tags differ.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import struct
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

MAX_BINARY = 1 << 30


@dataclass(frozen=True)
class Target:
    triple: str
    platform_tag: str  # compound tags joined with "."
    kind: str  # "elf", "macho" or "pe"
    machine: int  # ELF e_machine, Mach-O cputype or PE machine
    macos_min: tuple[int, int] | None = None

    @property
    def archive(self) -> str:
        return f"dynoxide-{self.triple}.zip" if self.kind == "pe" else f"dynoxide-{self.triple}.tar.gz"

    @property
    def member(self) -> str:
        return "dynoxide.exe" if self.kind == "pe" else "dynoxide"


def _linux(arch: str) -> str:
    return f"manylinux_2_17_{arch}.manylinux2014_{arch}.musllinux_1_1_{arch}"


TARGETS = (
    Target("x86_64-unknown-linux-musl", _linux("x86_64"), "elf", 62),
    Target("aarch64-unknown-linux-musl", _linux("aarch64"), "elf", 183),
    Target("x86_64-apple-darwin", "macosx_10_12_x86_64", "macho", 0x01000007, (10, 12)),
    Target("aarch64-apple-darwin", "macosx_11_0_arm64", "macho", 0x0100000C, (11, 0)),
    Target("x86_64-pc-windows-msvc", "win_amd64", "pe", 0x8664),
)


class BuildError(Exception):
    pass


def build(pure: Path, assets: Path, out: Path, targets: tuple[Target, ...] = TARGETS) -> list[Path]:
    sums = _read_sums(assets / "sha256sums.txt")
    binaries = []
    # Every archive is checked before any wheel is written, so a bad asset
    # leaves nothing half-built behind.
    for target in targets:
        archive = assets / target.archive
        _verify(archive, sums)
        binary = _extract(archive, target.member)
        _check_binary(binary, target)
        binaries.append((target, binary))
    out.mkdir(parents=True, exist_ok=True)
    return [_write_wheel(pure, target, binary, out) for target, binary in binaries]


def _read_sums(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise BuildError(f"{path} is missing")
    sums = {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            sums[parts[1].lstrip("*")] = parts[0].lower()
    return sums


def _verify(archive: Path, sums: dict[str, str]) -> None:
    if not archive.is_file():
        raise BuildError(f"{archive} is missing")
    expected = sums.get(archive.name)
    if expected is None:
        raise BuildError(f"sha256sums.txt has no line for {archive.name}")
    actual = hashlib.sha256(archive.read_bytes()).hexdigest()
    if actual != expected:
        raise BuildError(f"{archive.name} has SHA-256 {actual}, but sha256sums.txt says {expected}")


def _extract(archive: Path, member: str) -> bytes:
    """The one member, by exact name, refusing any other entry, a path, or a
    link."""
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive) as zf:
            infos = zf.infolist()
            if len(infos) != 1 or infos[0].filename != member:
                raise BuildError(f"{archive.name} holds {[i.filename for i in infos]}, not just {member}")
            info = infos[0]
            mode = info.external_attr >> 16
            if info.is_dir() or (mode and (mode & 0o170000) not in (0, 0o100000)):
                raise BuildError(f"{member} in {archive.name} isn't a regular file")
            if info.file_size > MAX_BINARY:
                raise BuildError(f"{member} in {archive.name} is too big")
            return zf.read(info)
    with tarfile.open(archive, "r:gz") as tf:
        members = tf.getmembers()
        if len(members) != 1 or members[0].name != member:
            raise BuildError(f"{archive.name} holds {[m.name for m in members]}, not just {member}")
        entry = members[0]
        if not entry.isreg():
            raise BuildError(f"{member} in {archive.name} isn't a regular file")
        if entry.size > MAX_BINARY:
            raise BuildError(f"{member} in {archive.name} is too big")
        data = tf.extractfile(entry)
        assert data is not None
        return data.read()


def _check_binary(data: bytes, target: Target) -> None:
    where = f"the {target.triple} binary"
    if target.kind == "elf":
        _check_elf(data, target, where)
    elif target.kind == "macho":
        _check_macho(data, target, where)
    else:
        _check_pe(data, target, where)


def _check_elf(data: bytes, target: Target, where: str) -> None:
    if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
        raise BuildError(f"{where} isn't a 64-bit little-endian ELF executable")
    (machine,) = struct.unpack_from("<H", data, 18)
    if machine != target.machine:
        raise BuildError(f"{where} is for ELF machine {machine}, not {target.machine}")
    (phoff,) = struct.unpack_from("<Q", data, 32)
    phentsize, phnum = struct.unpack_from("<HH", data, 54)
    for i in range(phnum):
        (kind,) = struct.unpack_from("<I", data, phoff + i * phentsize)
        if kind == 3:  # PT_INTERP: it needs a dynamic loader
            raise BuildError(
                f"{where} is dynamically linked, so one wheel can't carry both manylinux and musllinux tags"
            )


def _check_macho(data: bytes, target: Target, where: str) -> None:
    if data[:4] != b"\xcf\xfa\xed\xfe":
        raise BuildError(f"{where} isn't a thin 64-bit Mach-O executable")
    cputype, _, _, ncmds = struct.unpack_from("<IIII", data, 4)
    if cputype != target.machine:
        raise BuildError(f"{where} is for CPU type {cputype:#x}, not {target.machine:#x}")
    offset = 32
    minimum = None
    for _ in range(ncmds):
        cmd, size = struct.unpack_from("<II", data, offset)
        if cmd == 0x32:  # LC_BUILD_VERSION
            (minimum,) = struct.unpack_from("<I", data, offset + 12)
        elif cmd == 0x24:  # LC_VERSION_MIN_MACOSX
            (minimum,) = struct.unpack_from("<I", data, offset + 8)
        offset += size
    if minimum is None:
        raise BuildError(f"{where} doesn't say which macOS it needs")
    needs = (minimum >> 16, (minimum >> 8) & 0xFF)
    assert target.macos_min is not None
    if needs > target.macos_min:
        raise BuildError(
            f"{where} needs macOS {needs[0]}.{needs[1]}, but its wheel would be tagged {target.platform_tag}"
        )


def _check_pe(data: bytes, target: Target, where: str) -> None:
    if data[:2] != b"MZ":
        raise BuildError(f"{where} isn't a Windows executable")
    (pe,) = struct.unpack_from("<I", data, 0x3C)
    if data[pe : pe + 4] != b"PE\0\0":
        raise BuildError(f"{where} isn't a Windows executable")
    (machine,) = struct.unpack_from("<H", data, pe + 4)
    if machine != target.machine:
        raise BuildError(f"{where} is for PE machine {machine:#x}, not {target.machine:#x}")


def _write_wheel(pure: Path, target: Target, binary: bytes, out: Path) -> Path:
    with zipfile.ZipFile(pure) as zf:
        infos = zf.infolist()
        dist_info = _dist_info(pure, infos)
        name, version = dist_info.removesuffix(".dist-info").rsplit("-", 1)
        wheel_path = out / f"{name}-{version}-py3-none-{target.platform_tag}.whl"
        files: list[tuple[zipfile.ZipInfo, bytes]] = []
        for info in infos:
            if info.filename in (f"{dist_info}/RECORD", f"{dist_info}/WHEEL"):
                continue
            if info.filename.startswith(f"{name}/_bin/"):
                raise BuildError(f"{pure.name} already has a binary in it")
            files.append((info, zf.read(info)))
        wheel_text = zf.read(f"{dist_info}/WHEEL").decode()
    stamp = infos[0].date_time

    binary_info = zipfile.ZipInfo(f"{name}/_bin/{target.member}", stamp)
    binary_info.external_attr = 0o100755 << 16
    files.append((binary_info, binary))

    wheel_info = zipfile.ZipInfo(f"{dist_info}/WHEEL", stamp)
    wheel_info.external_attr = 0o100644 << 16
    files.append((wheel_info, _wheel_file(wheel_text, target).encode()))

    record = io.StringIO()
    for info, data in files:
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        record.write(f"{info.filename},sha256={digest},{len(data)}\n")
    record.write(f"{dist_info}/RECORD,,\n")
    record_info = zipfile.ZipInfo(f"{dist_info}/RECORD", stamp)
    record_info.external_attr = 0o100644 << 16
    files.append((record_info, record.getvalue().encode()))

    tmp = wheel_path.with_suffix(".tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
        for info, data in files:
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)
    tmp.replace(wheel_path)
    return wheel_path


def _dist_info(pure: Path, infos: list[zipfile.ZipInfo]) -> str:
    dirs = {i.filename.split("/", 1)[0] for i in infos if i.filename.split("/", 1)[0].endswith(".dist-info")}
    if len(dirs) != 1:
        raise BuildError(f"{pure.name} doesn't have exactly one .dist-info directory")
    dist_info = dirs.pop()
    if not pure.name.endswith("-py3-none-any.whl"):
        raise BuildError(f"{pure.name} isn't a pure py3-none-any wheel")
    return dist_info


def _wheel_file(text: str, target: Target) -> str:
    kept = [line for line in text.splitlines() if line.strip() and not line.startswith(("Tag:", "Root-Is-Purelib:"))]
    kept.append("Root-Is-Purelib: false")
    kept += [f"Tag: py3-none-{tag}" for tag in target.platform_tag.split(".")]
    return "\n".join(kept) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pure", type=Path, required=True, help="the py3-none-any wheel hatchling built")
    parser.add_argument("--assets", type=Path, required=True, help="the release archives and sha256sums.txt")
    parser.add_argument("--out", type=Path, required=True, help="where the platform wheels go")
    args = parser.parse_args(argv)
    try:
        wheels = build(args.pure, args.assets, args.out)
    except BuildError as err:
        print(f"build_wheels: {err}", file=sys.stderr)
        return 1
    for wheel in wheels:
        print(wheel)
    return 0


if __name__ == "__main__":
    sys.exit(main())
