from agent.base import Base


class Site(Base):
    def __init__(self, name, bench):
        self.name = name
        self.bench = bench
