import iptc

from agent.job import job, step
from agent.server import Server


class Firewall(Server):
    CHAIN_MAIN = "Frappe"
    CHAIN_BYPASS = "FrappeBypass"
    CHAIN_INPUT = "INPUT"

    def __init__(self, directory=None):
        super().__init__(directory)

    @job("Setup Firewall")
    def setup(self):
        self.setup_main()
        self.setup_bypass()
        self.link_input()

    @step("Setup Main Chain")
    def setup_main(self):
        table = self.table()
        table.create_chain(self.CHAIN_MAIN)
        table.commit()

    @step("Setup Bypass Chain")
    def setup_bypass(self):
        table = self.table()
        table.create_chain(self.CHAIN_BYPASS)
        table.commit()

    @step("Link Input Chain")
    def link_input(self):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_INPUT)
        for chain_target in (self.CHAIN_MAIN, self.CHAIN_BYPASS):
            rule = iptc.Rule()
            rule.target = iptc.Target(rule, chain_target)
            chain.insert_rule(rule)
        table.commit()

    @job("Teardown Firewall")
    def teardown(self):
        self.remove_main()
        self.remove_bypass()
        self.unlink_input()

    @step("Remove Main Chain")
    def remove_main(self):
        table = self.table()
        table.delete_chain(self.CHAIN_MAIN)
        table.commit()

    @step("Remove Bypass Chain")
    def remove_bypass(self):
        table = self.table()
        table.delete_chain(self.CHAIN_BYPASS)
        table.commit()

    @step("Unlink Input Chain")
    def unlink_input(self):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_INPUT)
        for rule in chain.rules:
            if rule.target.name in (self.CHAIN_MAIN, self.CHAIN_BYPASS):
                chain.delete_rule(rule)
        table.commit()

    @job("Sync Upstream")
    def sync(self, status: bool, rules: list[dict]):
        self.toggle(status)
        self.clear_rules()
        self.sync_rules(rules)

    @step("Toggle Firewall")
    def toggle(self, status: bool):
        if status:
            self.enable()
        else:
            self.disable()

    @step("Enable Firewall")
    def enable(self):
        self.unlink_input()
        self.link_input()

    @step("Disable Firewall")
    def disable(self):
        self.unlink_input()

    @step("Clear Rules")
    def clear_rules(self):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_MAIN)
        for rule in chain.rules:
            chain.delete_rule(rule)
        table.commit()

    @step("Sync Rules")
    def sync_rules(self, rules: list[dict]):
        for rule in rules:
            self.add_rule(
                source=rule.get("source"),
                destination=rule.get("destination"),
                action=rule.get("action"),
            )

    @step("Add Rule")
    def add_rule(self, source: str, destination: str, action: str):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_MAIN)
        rule = iptc.Rule()
        rule.src = source
        rule.dst = destination
        rule.target = iptc.Target(rule, self.transform_action(action))
        chain.insert_rule(rule)
        table.commit()

    @step("Remove Rule")
    def remove_rule(self, source: str, destination: str, action: str):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_MAIN)
        action = self.transform_action(action)
        for rule in chain.rules:
            if rule.src == source and rule.dst == destination and rule.target.name == action:
                chain.delete_rule(rule)
        table.commit()

    def table(self) -> iptc.Table:
        return iptc.Table(iptc.Table.FILTER)

    def transform_action(self, action: str) -> str:
        _map = {
            "ACCEPT": "Allow",
            "DROP": "Block",
        }
        _map_reversed = {v: k for k, v in _map.items()}
        _map_merged = {**_map, **_map_reversed}
        return _map_merged.get(action, action)
