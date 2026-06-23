"""Supply-chain integrity tests for the bypass_unlink LD_PRELOAD shim.

The shim is LD_PRELOAD-ed into the in-container bench process during a streaming
backup (see ``Site.backup``). Because it is injected into a production process,
it could intercept *arbitrary* libc calls. Rather than commit a prebuilt ``.so``
- a binary artifact that could be silently swapped with no source diff - the
shim is compiled from ``lib/bypass_unlink.c`` via ``lib/Makefile`` on every
backup (see ``Site.build_bypass_unlink_shim``). The trust surface is therefore
the small, reviewable C source. These tests are the guard against that source
(or the build) growing reach it should not have:

1. The shim must export ONLY the functions it is supposed to interpose
   (``unlink``/``unlinkat``) - nothing else.
2. The shim must import ONLY the externals its source actually uses
   (``strstr``/``dlsym``) - so a source change that reaches for ``open``,
   ``execve``, ``socket``, ``system`` ... fails the test.

The ELF parsing is intentionally dependency-free (stdlib only). The build needs
make + gcc, which CI installs; locally the binary-level checks skip when the
toolchain is absent (the source-level check still runs unconditionally).
"""

from __future__ import annotations

import os
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIB_DIR = os.path.join(REPO_ROOT, "lib")
SOURCE = os.path.join(LIB_DIR, "bypass_unlink.c")

ET_DYN = 3  # shared object

# The shim must interpose EXACTLY these libc functions. Changing this set is a
# deliberate, security-relevant act: update it here AND in the C source.
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
        binding = st_info >> 4  # STB_LOCAL == 0, STB_GLOBAL == 1, STB_WEAK == 2
        if struct.unpack_from("<H", data, o + 6)[0] == 0:  # SHN_UNDEF -> imported
            # Only GLOBAL undefined symbols are real external dependencies. gcc
            # emits weak undefined boilerplate (__gmon_start__, __cxa_finalize,
            # _ITM_*registerTMCloneTable) into every shared object as startup/ABI
            # glue, not as calls the source makes - and a genuine call to
            # system/execve/... always produces a GLOBAL reference, never a weak
            # one, so the security check loses nothing by skipping weak symbols.
            if binding == 1:  # STB_GLOBAL
                imported.add(name)
            continue
        # STT_FUNC == 2; STB_GLOBAL == 1, STB_WEAK == 2
        if (st_info & 0xF) == 2 and binding in (1, 2):
            exported.add(name)
    return exported, imported


def _read_dynamic_symbols(path):
    """Parse an ELF64 LE shared object using only the stdlib (no pyelftools).

    Returns (e_type, exported_funcs, imported_syms).
    """
    with open(path, "rb") as f:
        data = f.read()

    if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
        raise ValueError(f"{path}: expected 64-bit little-endian ELF")

    e_type = struct.unpack_from("<H", data, 16)[0]
    sections = _elf_sections(data)
    if ".dynsym" not in sections or ".dynstr" not in sections:
        raise ValueError(f"{path}: missing .dynsym/.dynstr")
    exported, imported = _iter_dynsym(data, sections)
    return e_type, exported, imported


class TestBypassUnlinkShim(unittest.TestCase):
    """Integrity of the bypass_unlink LD_PRELOAD shim built from source."""

    @classmethod
    def setUpClass(cls):
        """Build the shim once via the Makefile, exactly as Site does.

        Gated on Linux: the shim is only ever built and LD_PRELOAD-ed on the
        Linux agent host, and a native build elsewhere (e.g. a Mach-O on macOS)
        is not the ELF these checks parse. CI runs on Linux, so the binary-level
        guarantees are always enforced there.
        """
        cls._tmp = None
        cls._built = None
        if not sys.platform.startswith("linux"):
            return
        if not (shutil.which("make") and shutil.which("gcc")):
            return
        cls._tmp = tempfile.TemporaryDirectory()
        built = os.path.join(cls._tmp.name, "bypass_unlink.so")
        subprocess.run(
            ["make", "-C", LIB_DIR, f"OUT={built}"],
            check=True,
            capture_output=True,
        )
        cls._built = built

    @classmethod
    def tearDownClass(cls):
        if cls._tmp is not None:
            cls._tmp.cleanup()

    def _require_build(self):
        if self._built is None:
            self.skipTest("shim not built (non-Linux or make/gcc absent); source-level check still enforced")
        return self._built

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

    def test_builds_a_shared_object(self):
        built = self._require_build()
        e_type, _, _ = _read_dynamic_symbols(built)
        self.assertEqual(e_type, ET_DYN, "build did not produce a shared object")

    def test_interposes_only_expected_functions(self):
        """A source change that interposes extra libc calls fails here."""
        built = self._require_build()
        _, exported, _ = _read_dynamic_symbols(built)
        self.assertEqual(
            exported,
            EXPECTED_EXPORTS,
            f"shim exports {sorted(exported)}, expected {sorted(EXPECTED_EXPORTS)}. "
            "The shim must interpose ONLY these functions.",
        )

    def test_imports_only_expected_externals(self):
        """A source change reaching for open/execve/socket/system fails here."""
        built = self._require_build()
        _, _, imported = _read_dynamic_symbols(built)
        self.assertEqual(
            imported,
            EXPECTED_IMPORTS,
            f"shim imports {sorted(imported)}, expected {sorted(EXPECTED_IMPORTS)}. "
            "The shim must not call into libc beyond what the source does.",
        )

    def test_no_debug_strings(self):
        """Guard against a debug printf creeping back into the binary."""
        built = self._require_build()
        with open(built, "rb") as f:
            blob = f.read()
        self.assertNotIn(b"hi (blocked", blob, "leftover debug string in shim")


if __name__ == "__main__":
    unittest.main()
