import os
import importlib


class PatchHandler:
    def __init__(self, patch=None, path=None):
        self.patch = patch
        self.path = path
        self._executed_patches = set()

    @property
    def executed_patches(self):
        if not self._executed_patches:
            self._executed_patches = set(self.retrieve_patches())
            # self._executed_patches = set()
            return self._executed_patches

    def retrieve_patches(self):
        from agent.job import PatchLogModel

        patches = PatchLogModel.select()
        return [patch.patch for patch in patches]

    def execute(self):
        if self.patch not in self.executed_patches:
            print("Executing patch", self.patch)
            try:
                self.get_method()()
            except Exception as e:
                print("Failed to execute patch", self.patch)
                raise e
            else:
                self.log_patch()

    def get_method(self, attr="execute"):
        _patch = self.patch.split(maxsplit=1)[0]
        module = importlib.import_module(_patch)
        return getattr(module, attr)

    def log_patch(self):
        from agent.job import PatchLogModel

        patch_log = PatchLogModel()
        patch_log.patch = self.patch
        patch_log.save()


def run_patches():
    directory = os.getcwd()
    patches_dir = f"{directory}/repo/agent/patches.txt"

    if not _patch_log_exists():
        print("Creating patch log")
        _create_patch_log()

    with open(patches_dir, "r") as f:
        patches = f.readlines()
        for patch in patches:
            patch = patch.strip()
            patch_path = f"{directory}/patches/{patch}"

            patch_handler = PatchHandler(patch=patch, path=patch_path)
            patch_handler.execute()


def _patch_log_exists():
    from agent.job import agent_database as database

    tables = database.get_tables()

    if "patchlogmodel" in tables:
        return True

    return False


def _create_patch_log():
    from agent.job import PatchLogModel

    PatchLogModel.create_table()
