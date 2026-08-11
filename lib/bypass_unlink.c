#define _GNU_SOURCE
#include <string.h>
#include <dlfcn.h>
#include <fcntl.h>

int unlink(const char *path)
{
	if (strstr(path, "%stream%") != NULL)
	{
		return 0;
	}

	int (*real_unlink)(const char *) = dlsym(RTLD_NEXT, "unlink");
	return real_unlink(path);
}

int unlinkat(int dirfd, const char *path, int flags)
{
	if (strstr(path, "%stream%") != NULL)
	{
		return 0;
	}

	int (*real_unlinkat)(int, const char *, int) = dlsym(RTLD_NEXT, "unlinkat");
	return real_unlinkat(dirfd, path, flags);
}
