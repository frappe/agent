import os
import typing

import filelock

if typing.TYPE_CHECKING:
    from agent.server import Server


class NFSHandler:
    def __init__(self, server: "Server"):
        self.server = server
        self.exports_file = "/home/frappe/exports"
        self.shared_directory = "/home/frappe/nfs"
        self.options = "rw,sync,no_subtree_check"

    def reload_exports(self):
        self.server.execute("sudo exportfs -ra")

    def add_to_acl(
        self,
        primary_server_private_ip: str,
        secondary_server_private_ip: str,
        shared_directory: str,
    ):
        """
        Updates the exports file on the nfs host server
        """
        server_shared_directory = os.path.join(self.shared_directory, shared_directory)

        os.makedirs(server_shared_directory)
        self.server.execute(f"chown -R frappe:frappe {server_shared_directory}")

        lock = filelock.SoftFileLock(self.exports_file + ".lock")

        with lock.acquire(timeout=10), open(self.exports_file, "a+") as f:
            f.write(f"{server_shared_directory} {primary_server_private_ip}({self.options})\n")
            f.write(f"{server_shared_directory} {secondary_server_private_ip}({self.options})\n")

        self.reload_exports()

    def remove_from_acl(
        self, shared_directory: str, primary_server_private_ip: str, secondary_server_private_ip: str
    ):
        """Unsubscrible a given private IP from a give file system"""
        server_shared_directory = os.path.join(self.shared_directory, shared_directory)
        remove_lines = [
            f"{server_shared_directory} {primary_server_private_ip}({self.options})",
            f"{server_shared_directory} {secondary_server_private_ip}({self.options})",
        ]
        for line in remove_lines:
            lock = filelock.SoftFileLock(self.exports_file + ".lock")
            with lock.acquire(timeout=10):
                self.server.execute(f"sed -i '\\|{line}|d' {self.exports_file}")

        self.reload_exports()
