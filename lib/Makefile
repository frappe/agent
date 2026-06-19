# Reproducible build of the bypass_unlink LD_PRELOAD shim.
#
# The shim is preloaded into the in-container bench process during a streaming
# backup to stop the framework deleting the %stream% FIFOs mid-stream. It must
# match the container architecture, so we ship a prebuilt .so per arch.
#
# Built with `zig cc` so the binaries cross-compile identically from any host
# (macOS/Linux, x86_64/arm64). We pin an old glibc (2.31) for broad container
# compatibility; the shim only references GLIBC_2.2.5 symbols in practice.
#
# Usage: `make` (rebuild both), `make clean`.

CC      := zig cc
CFLAGS  := -shared -fPIC -O2
SRC     := bypass_unlink.c
OUT     := bin

all: $(OUT)/bypass_unlink_x86_64.so $(OUT)/bypass_unlink_arm64.so

$(OUT)/bypass_unlink_x86_64.so: $(SRC) | $(OUT)
	$(CC) -target x86_64-linux-gnu.2.31 $(CFLAGS) -o $@ $<

$(OUT)/bypass_unlink_arm64.so: $(SRC) | $(OUT)
	$(CC) -target aarch64-linux-gnu.2.31 $(CFLAGS) -o $@ $<

$(OUT):
	mkdir -p $(OUT)

clean:
	rm -f $(OUT)/bypass_unlink_x86_64.so $(OUT)/bypass_unlink_arm64.so

.PHONY: all clean
