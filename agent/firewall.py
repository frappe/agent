import iptc

from agent.base import Base
from agent.job import job, step


class Firewall(Base):
    CHAIN_MAIN = "Frappe"
    CHAIN_BYPASS = "FrappeBypass"
    CHAIN_INPUT = "INPUT"

    def __init__(self, directory=None):
        super().__init__()
        self.job = None
        self.step = None

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

    def teardown(self):
        self.remove_main()
        self.remove_bypass()
        self.unlink_input()

    def remove_main(self):
        table = self.table()
        table.delete_chain(self.CHAIN_MAIN)
        table.commit()

    def remove_bypass(self):
        table = self.table()
        table.delete_chain(self.CHAIN_BYPASS)
        table.commit()

    def unlink_input(self):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_INPUT)
        for rule in chain.rules:
            if rule.target.name in (self.CHAIN_MAIN, self.CHAIN_BYPASS):
                chain.delete_rule(rule)
        table.commit()

    def enable(self):
        self.unlink_input()
        self.link_input()

    def disable(self):
        self.unlink_input()

    def add_rule(self, source: str, destination: str, action: str):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_MAIN)
        rule = iptc.Rule()
        rule.src = source
        rule.dst = destination
        rule.target = iptc.Target(rule, action)
        chain.insert_rule(rule)
        table.commit()

    def remove_rule(self, source: str, destination: str, action: str):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_MAIN)
        for rule in chain.rules:
            if rule.src == source and rule.dst == destination and rule.target.name == action:
                chain.delete_rule(rule)
        table.commit()

    def status(self):
        return {
            "enabled": self.is_enabled(),
            "rules": list(self.rules()),
        }

    def is_enabled(self) -> bool:
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_INPUT)
        for rule in chain.rules:
            if rule.target.name == self.CHAIN_MAIN:
                return True
        return False

    def rules(self):
        table = self.table()
        chain = iptc.Chain(table, self.CHAIN_MAIN)
        for rule in chain.rules:
            yield {
                "source": self.pretty_ip(rule.src),
                "destination": self.pretty_ip(rule.dst),
                "action": rule.target.name,
            }

    def table(self) -> iptc.Table:
        return iptc.Table(iptc.Table.FILTER)

    def pretty_ip(self, ip: str) -> str:
        return ip.split("/").pop(0)
