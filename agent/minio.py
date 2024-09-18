from __future__ import annotations

import os
from agent.job import job, step
from agent.server import Server


class Minio(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.policy_path = "/home/frappe/minio/tmp_policy.json"
        self.host = "localhost"
        self.job = None
        self.step = None

    @job("Create Minio User")
    def create_subscription(
        self, access_key, secret_key, policy_name, policy_json
    ):
        self.create_user(access_key, secret_key)
        self.create_policy(policy_name, policy_json)
        self.add_policy(access_key, policy_name)

    @step("Create Minio User")
    def create_user(self, access_key, secret_key):
        # access_key = username on minio
        self.execute(
            f"mc admin user add {self.host} {access_key} {secret_key}"
        )

    @step("Create Minio Policy")
    def create_policy(self, policy_name, policy_json):
        self.execute(f"echo '{policy_json}' > {self.policy_path}")
        self.execute(
            f"mc admin policy add {self.host} {policy_name} {self.policy_path}"
        )

    @step("Add Minio Policy")
    def add_policy(self, access_key, policy_name):
        self.execute(
            f"mc admin policy set {self.host} {policy_name} user={access_key}"
        )

    @job("Disable Minio User")
    def disable_user(self, username):
        self.disable(username)

    @step("Disable Minio User")
    def disable(self, username):
        self.execute(f"mc admin user disable {self.host} {username}")

    @job("Enable Minio User")
    def enable_user(self, username):
        self.enable(username)

    @step("Enable Minio User")
    def enable(self, username):
        self.execute(f"mc admin user enable {self.host} {username}")

    @job("Remove Minio User")
    def remove_user(self, username):
        self.remove(username)

    @step("Remove Minio User")
    def remove(self, username):
        self.execute(f"mc admin user remove {self.host} {username}")
