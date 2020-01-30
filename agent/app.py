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
        return self.execute(f"git reset --hard {abbreviation}")

    def fetch(self):
        self.execute(f"git fetch {self.remote}")

    @property
    def remote(self):
        remotes = self.execute("git remote").split("\n")
        if "upstream" in remotes:
            return "upstream"
        if "origin" in remotes:
            return "origin"
        raise Exception(f"Invalid remote for {self.directory}")
