# Agent

## Installation
```
mkdir agent && cd agent
git clone https://github.com/frappe/agent repo
virtualenv env
source env/bin/activate
pip install -e ./repo
cp repo/Procfile .
```

## Running
```
honcho start
```
