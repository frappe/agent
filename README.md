# Agent

## Installation

```
mkdir agent && cd agent
git clone https://github.com/frappe/agent repo
virtualenv env
source env/bin/activate
pip install -e ./repo
cp repo/redis.conf .
cp repo/Procfile .
```

## Running

```
honcho start
```

## CLI

Agent has a CLI interface
([ref](https://github.com/frappe/agent/blob/master/agent/cli.py)). You can
access this by activating the env:

```bash
# Path to your agent's Python env might be different
source ./agent/env/bin/activate

agent --help
```

Once you have activated the env, you can access the iPython console:

```bash
agent console
```

This should have the server object instantiated if it was able to find the
`config.json` file. If not you can specify the path (check `agent console --help`).
