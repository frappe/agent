# Native build of the bypass_unlink LD_PRELOAD shim.
#
# The shim is preloaded into the in-container bench process during a streaming
# backup to stop the framework deleting the %stream% FIFOs mid-stream. It is
# NOT committed as a prebuilt binary - shipping a .so would be a swappable
# supply-chain artifact. Instead it is compiled from source on every backup by
# Site.build_bypass_unlink_shim, which just calls:
#
#     make -C lib OUT=<absolute path to the .so to produce>
#
# A native build needs no arch juggling: Docker on Linux shares the host
# architecture, and glibc is backward-compatible, so a shim built against the
# host's (same-or-older) glibc loads inside the container. The shim only touches
# strstr/dlsym, the most stable libc symbols.
#
# test_bypass_unlink.py rebuilds via this same target and asserts the result
# interposes/imports only the allowed symbols.

CC     ?= gcc
CFLAGS := -shared -fPIC -O2 -s
SRC    := bypass_unlink.c

ifndef OUT
$(error OUT=<output .so path> is required, e.g. `make OUT=/tmp/bypass_unlink.so`)
endif

all: $(OUT)

$(OUT): $(SRC)
	$(CC) $(CFLAGS) -o $@ $<

.PHONY: all
