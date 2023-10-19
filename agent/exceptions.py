
   
class BenchNotExistsException(Exception):
    def __init__(self, bench):
        self.bench = bench

    def __str__(self):
        return f"Bench {self.bench} does not exist"

class SiteNotExistsException(Exception):
    def __init__(self, site, bench):
        self.site = site
        self.bench = bench

    def __str__(self):
        return f"Site {self.site} does not exist on bench {self.bench}" 