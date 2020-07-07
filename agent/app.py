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
        # Automatically unshallow repository while fetching
        shallow = self.execute("git rev-parse --is-shallow-repository")[
            "output"
        ]
        unshallow = "--unshallow" if shallow == "true" else ""
        return self.execute(f"git fetch {self.remote} {unshallow}")

    def fetch_ref(self, ref):
        return self.execute(
            f"git fetch --progress --depth 1 {self.remote} {ref}"
        )

    def checkout(self, ref):
        return self.execute(f"git checkout {ref}")

    @property
    def remote(self):
        remotes = self.execute("git remote")["output"].split("\n")
        if "upstream" in remotes:
            return "upstream"
        if "origin" in remotes:
            return "origin"
        raise Exception(f"Invalid remote for {self.directory}")
