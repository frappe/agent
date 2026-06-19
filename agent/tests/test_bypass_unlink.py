"""Supply-chain integrity tests for the prebuilt LD_PRELOAD shim binaries.

`lib/bin/bypass_unlink_*.so` are committed binaries that get LD_PRELOAD-ed
into the in-container bench process during a streaming backup (see
``Site.backup``). Because they are injected into a production process, a silent
swap of a committed ``.so`` could intercept *arbitrary* libc calls with no
visible source diff. These tests are the guard against that:

1. The shim must export ONLY the functions it is supposed to interpose
   (``unlink``/``unlinkat``) - nothing else.
2. The shim must import ONLY the externals its source actually uses
   (``strstr``/``dlsym``) - so a swapped binary that reaches for ``open``,
   ``execve``, ``socket``, ``system`` ... fails the test.
3. The committed binaries must be byte-for-byte reproducible from
   ``lib/bypass_unlink.c`` via the pinned toolchain (verified when ``zig`` +
   ``make`` are present; always enforced in CI).

The ELF parsing is intentionally dependency-free (stdlib only) so the symbol
checks run unconditionally in CI, even where the build toolchain is absent.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import struct
import subprocess
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIB_DIR = os.path.join(REPO_ROOT, "lib")
SOURCE = os.path.join(LIB_DIR, "bypass_unlink.c")
BIN_DIR = os.path.join(LIB_DIR, "bin")

# ELF e_machine values for the architectures we ship.
EM_X86_64 = 62
EM_AARCH64 = 183
ET_DYN = 3  # shared object

BINARIES = {
    "bypass_unlink_x86_64.so": EM_X86_64,
    "bypass_unlink_arm64.so": EM_AARCH64,
}

# The shim must interpose EXACTLY these libc functions. Changing this set is a
# deliberate, security-relevant act: update it here AND rebuild the binaries.
EXPECTED_EXPORTS = {"unlink", "unlinkat"}

# The only externals the shim is allowed to import. strstr matches the marker;
# dlsym resolves the real libc function. Anything else means the binary calls
# into libc beyond what the source does.
EXPECTED_IMPORTS = {"strstr", "dlsym"}


def _cstr(data, base, idx):
    """Read a NUL-terminated string at ``base + idx`` from an ELF string table."""
    end = data.index(b"\x00", base + idx)
    return data[base + idx : end].decode()


def _elf_sections(data):
    """Return {section_name: (file_offset, size)} for an ELF64 LE file."""
    e_shoff = struct.unpack_from("<Q", data, 40)[0]
    e_shentsize = struct.unpack_from("<H", data, 58)[0]
    e_shnum = struct.unpack_from("<H", data, 60)[0]
    e_shstrndx = struct.unpack_from("<H", data, 62)[0]

    raw = []  # (name_index, file_offset, size)
    for i in range(e_shnum):
        off = e_shoff + i * e_shentsize
        raw.append(
            (
                struct.unpack_from("<I", data, off)[0],
                struct.unpack_from("<Q", data, off + 24)[0],
                struct.unpack_from("<Q", data, off + 32)[0],
            )
        )
    shstr_off = raw[e_shstrndx][1]
    return {_cstr(data, shstr_off, name): (off, size) for (name, off, size) in raw}


def _iter_dynsym(data, sections):
    """Return (exported_funcs, imported_syms) from .dynsym/.dynstr."""
    dynsym_off, dynsym_size = sections[".dynsym"]
    dynstr_off, _ = sections[".dynstr"]

    exported, imported = set(), set()
    for i in range(dynsym_size // 24):  # Elf64_Sym is 24 bytes
        o = dynsym_off + i * 24
        name = _cstr(data, dynstr_off, struct.unpack_from("<I", data, o)[0])
        if not name:
            continue
        st_info = data[o + 4]
        if struct.unpack_from("<H", data, o + 6)[0] == 0:  # SHN_UNDEF -> imported
            imported.add(name)
            continue
        # STT_FUNC == 2; STB_GLOBAL == 1, STB_WEAK == 2
        if (st_info & 0xF) == 2 and (st_info >> 4) in (1, 2):
            exported.add(name)
    return exported, imported


def _read_dynamic_symbols(path):
    """Parse an ELF64 LE shared object using only the stdlib (no pyelftools).

    Returns (e_machine, e_type, exported_funcs, imported_syms).
    """
    with open(path, "rb") as f:
        data = f.read()

    if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
        raise ValueError(f"{path}: expected 64-bit little-endian ELF")

    e_type = struct.unpack_from("<H", data, 16)[0]
    e_machine = struct.unpack_from("<H", data, 18)[0]
    sections = _elf_sections(data)
    if ".dynsym" not in sections or ".dynstr" not in sections:
        raise ValueError(f"{path}: missing .dynsym/.dynstr")
    exported, imported = _iter_dynsym(data, sections)
    return e_machine, e_type, exported, imported


class TestBypassUnlinkShim(unittest.TestCase):
    """Integrity of the committed LD_PRELOAD shim binaries."""

    def test_source_defines_exactly_the_expected_interposers(self):
        """The C source must define exactly the functions we expect to interpose."""
        with open(SOURCE) as f:
            src = f.read()
        for name in EXPECTED_EXPORTS:
            self.assertIn(
                f"int {name}(",
                src,
                f"{name} is in EXPECTED_EXPORTS but not defined in bypass_unlink.c",
            )

    def test_binaries_exist(self):
        for name in BINARIES:
            self.assertTrue(
                os.path.isfile(os.path.join(BIN_DIR, name)),
                f"committed shim missing: {name}",
            )

    def test_binaries_are_expected_shared_objects(self):
        for name, machine in BINARIES.items():
            e_machine, e_type, _, _ = _read_dynamic_symbols(os.path.join(BIN_DIR, name))
            self.assertEqual(e_type, ET_DYN, f"{name}: not a shared object")
            self.assertEqual(e_machine, machine, f"{name}: wrong architecture")

    def test_interposes_only_expected_functions(self):
        """A swapped binary that interposes extra libc calls fails here."""
        for name in BINARIES:
            _, _, exported, _ = _read_dynamic_symbols(os.path.join(BIN_DIR, name))
            self.assertEqual(
                exported,
                EXPECTED_EXPORTS,
                f"{name} exports {sorted(exported)}, expected {sorted(EXPECTED_EXPORTS)}. "
                "The shim must interpose ONLY these functions.",
            )

    def test_imports_only_expected_externals(self):
        """A swapped binary reaching for open/execve/socket/system fails here."""
        for name in BINARIES:
            _, _, _, imported = _read_dynamic_symbols(os.path.join(BIN_DIR, name))
            self.assertEqual(
                imported,
                EXPECTED_IMPORTS,
                f"{name} imports {sorted(imported)}, expected {sorted(EXPECTED_IMPORTS)}. "
                "The shim must not call into libc beyond what the source does.",
            )

    def test_no_debug_strings(self):
        """Guard against the old debug printf creeping back into the binary."""
        for name in BINARIES:
            with open(os.path.join(BIN_DIR, name), "rb") as f:
                blob = f.read()
            self.assertNotIn(b"hi (blocked", blob, f"{name}: leftover debug string")

    def test_committed_binaries_match_source_rebuild(self):
        """Rebuild from source with the pinned toolchain; bytes must match.

        This is the full source<->binary guarantee. It needs `zig` + `make`,
        which CI installs; locally it skips when the toolchain is absent (the
        symbol-level tests above still run unconditionally).
        """
        if not shutil.which("zig") or not shutil.which("make"):
            self.skipTest("zig/make not available; symbol-level checks still enforced")

        with tempfile.TemporaryDirectory() as out:
            subprocess.run(
                ["make", "-C", LIB_DIR, f"OUT={out}"],
                check=True,
                capture_output=True,
            )
            for name in BINARIES:
                committed = os.path.join(BIN_DIR, name)
                rebuilt = os.path.join(out, name)
                self.assertTrue(os.path.isfile(rebuilt), f"rebuild produced no {name}")
                with open(committed, "rb") as a, open(rebuilt, "rb") as b:
                    self.assertEqual(
                        hashlib.sha256(a.read()).hexdigest(),
                        hashlib.sha256(b.read()).hexdigest(),
                        f"{name} does not match a fresh build from bypass_unlink.c. "
                        "The committed binary may have been replaced without a source change.",
                    )


if __name__ == "__main__":
    unittest.main()
