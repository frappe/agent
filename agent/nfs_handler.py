import os
import typing

from agent.job import job, step

if typing.TYPE_CHECKING:
    from agent.server import Server


class NFSHandler:
    def __init__(self, server: "Server"):
        self.server = server
        self.shared_directory = "/home/frappe/nfs"

    def update_nfs_exports_on_host(
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

        with open("/home/frappe/exports", "a+") as f:
            f.write(f"{server_shared_directory} {private_ip_to_enable_mount_on}(rw,sync,no_subtree_check)\n")

        self.server.execute("sudo exportfs -ra")
