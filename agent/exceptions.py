
   
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
