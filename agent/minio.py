from agent.job import job, step
from agent.server import Server

class Minio(Server):
    def __init__(self):
        self.directory = "/home/minio/minio_policies/"
        self.alias = "localhost"

    @job("Create Minio Subscription")
    def create_subscription(self, access_key, secret_key, policy_name, policy_json):
        self.create_user(access_key, secret_key)
        self.create_policy(policy_name, policy_json)
        self.add_policy(access_key, policy_name)

    @step("Create Minio User")
    def create_user(self, access_key, secret_key):
        # access_key = username on minio
        self.execute(f"mc admin user add {self.alias} {access_key} {secret_key}")

    @step("Create Minio Policy")
    def create_policy(self, policy_name, policy_json):
        self.execute(f"echo '{policy_json}' >> {self.directory}{policy_name}_policy.json")
        self.execute(f"mc admin policy add {self.alias} {policy_name} {self.directory}{policy_name}_policy.json")

    @step("Add Policy to User")
    def add_policy(self, access_key, policy_name):
        self.execute(f"mc admin policy set {self.alias} {policy_name} user={access_key}")

    @job("Disable Minio Subscription")
    def disable_subscription(self, username):
        self.execute(f"mc admin user disable {self.alias} {username}")

    @job("Enable Minio Subscription")
    def enable_subscription(self, username):
        self.execute(f"mc admin user enable {self.alias} {username}")

    @job("Remove Minio User")
    def remove_user(self, username):
        self.execute(f"mc admin user remove {self.alias} {username}")
