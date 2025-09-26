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
        server_to_enable_mount_on: str,
        private_ip_to_enable_mount_on: str,
        use_file_system_of_server: str,
        share_file_system: bool,
    ):
        """
        Updates the exports file on the nfs host server
        """
        server_shared_directory = os.path.join(
            self.shared_directory,
            server_to_enable_mount_on if share_file_system else use_file_system_of_server,
        )
        if share_file_system:
            os.makedirs(server_shared_directory)
            self.server.execute(f"chown -R frappe:frappe {server_shared_directory}")

        lock = filelock.SoftFileLock(self.exports_file + ".lock")

        with lock.acquire(timeout=10), open(self.exports_file, "a+") as f:
            f.write(f"{server_shared_directory} {private_ip_to_enable_mount_on}({self.options})\n")

        self.reload_exports()

    def remove_from_acl(self, file_system: str, private_ip_to_disable_mount_on: str):
        """Unsubscrible a given private IP from a give file system"""
        server_shared_directory = os.path.join(self.shared_directory, file_system)
        remove_line = f"{server_shared_directory} {private_ip_to_disable_mount_on}({self.options})"

        lock = filelock.SoftFileLock(self.exports_file + ".lock")
        with lock.acquire(timeout=10):
            self.server.execute(f"sed -i '\\|{remove_line}|d' {self.exports_file}")

        self.reload_exports()
