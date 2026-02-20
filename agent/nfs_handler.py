import os
import typing

import filelock

if typing.TYPE_CHECKING:
    from agent.server import Server


class NFSHandler:
    def __init__(self, server: "Server"):
        self.server = server
        self.exports_file = "/home/frappe/exports"
        self.benches_directory = "/home/frappe/benches"
        self.shared_directory = "/home/frappe/shared"
        self.options = "rw,sync,no_subtree_check"

    def reload_exports(self):
        self.server.execute("sudo exportfs -ra")

    def add_to_acl(
        self,
        secondary_server_private_ip: str,
    ):
        """
        Updates the exports file on the nfs host server
        """
        os.makedirs(self.shared_directory, exist_ok=True)
        self.server.execute(f"chown -R frappe:frappe {self.shared_directory}")

        lock = filelock.FileLock(self.exports_file + ".lock")

        with lock.acquire(timeout=10), open(self.exports_file, "a+") as f:
            f.write(f"{self.benches_directory} {secondary_server_private_ip}({self.options})\n")

        self.reload_exports()

    def remove_from_acl(self, secondary_server_private_ip: str):
        """Unsubscrible a given private IP from a give file system"""
        remove_lines = [
            f"{self.benches_directory} {secondary_server_private_ip}({self.options})",
        ]
        for line in remove_lines:
            lock = filelock.FileLock(self.exports_file + ".lock")
            with lock.acquire(timeout=10):
                self.server.execute(f"sed -i '\\|{line}|d' {self.exports_file}")

        self.reload_exports()
