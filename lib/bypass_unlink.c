#define _GNU_SOURCE
#include <string.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>

int unlink(const char *path)
{
	if (strstr(path, "%stream%") != NULL)
	{
		return 0;
	}

	int (*real_unlink)(const char *) = dlsym(RTLD_NEXT, "unlink");
	int ret = real_unlink(path);
	/* os.remove on a directory: skip it. Every other error still fails. */
	return (ret != 0 && errno == EISDIR) ? 0 : ret;
}

int unlinkat(int dirfd, const char *path, int flags)
{
	if (strstr(path, "%stream%") != NULL)
	{
		return 0;
	}

	int (*real_unlinkat)(int, const char *, int) = dlsym(RTLD_NEXT, "unlinkat");
	int ret = real_unlinkat(dirfd, path, flags);
	/* Same for os.remove(dir_fd=...); AT_REMOVEDIR is os.rmdir, left alone. */
	if (ret != 0 && errno == EISDIR && !(flags & AT_REMOVEDIR))
	{
		return 0;
	}
	return ret;
}
