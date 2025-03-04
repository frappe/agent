from __future__ import annotations


class AgentException(Exception):
    def __init__(self, data):
        self.data = data


class BenchNotExistsException(Exception):
    def __init__(self, bench):
        self.bench = bench
        self.message = f"Bench {bench} does not exist"

        super().__init__(self.message)


class SiteNotExistsException(Exception):
    def __init__(self, site, bench):
        self.site = site
        self.bench = bench
        self.message = f"Site {site} does not exist on bench {bench}"

        super().__init__(self.message)


class InvalidSiteConfigException(AgentException):
    def __init__(self, data: dict, site=None):
        self.site = site
        super().__init__(data)
