import re
from agent.base import Base

class Security(Base):
    @property
    def logs_directory(self):
        return '/var/log/ssh_sessions'

    @property
    def ssh_session_logs(self):
        return self.logs

    def retrieve_ssh_session_log(self, filename):
        content = self.retrieve_log(filename)
        return self.escape_ansi(content)

    def escape_ansi(self, line):
        ansi_escape =re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
        return ansi_escape.sub('', line)

