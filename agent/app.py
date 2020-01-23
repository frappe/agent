import os

from agent.base import Base


class App(Base):
    def __init__(self, name, bench):
        self.name = name
        self.directory = os.path.join(bench.directory, "apps", name)
        if not os.path.isdir(self.directory):
            raise Exception
        self.execute("git rev-parse --is-inside-work-tree")

    def dump(self):
        return {"name": self.name}

    def execute(self, command):
        return super().execute(command, directory=self.directory)

    def reset(self, abbreviation="HEAD"):
        return self.execute(f"git reset {abbreviation}")
