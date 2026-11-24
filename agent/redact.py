from __future__ import annotations

import inspect
import re

REDACTED = "********"
VALUE = r"""('[^']*'|"[^"]*"|\S+)"""
# `--admin-password value` or `--password=value`; `--password-stdin` takes no value
PASSWORD_FLAG_PATTERN = re.compile(rf"(--(?![\w-]*-stdin\b)[\w-]*password[\w-]*(?:\s+|=))(?!-){VALUE}")
# `bench --site x set-admin-password value`
ADMIN_PASSWORD_PATTERN = re.compile(rf"(\bset-admin-password\s+)(?!-){VALUE}")
# `['tool', '--admin-password', 'value']` in a CalledProcessError message
LIST_PASSWORD_FLAG_PATTERN = re.compile(
    r"""(['"]--(?![\w-]*-stdin\b)[\w-]*password[\w-]*['"],\s*)('[^']*'|"[^"]*")"""
)
# `mariadb -u user -pvalue`
MYSQL_PASSWORD_PATTERN = re.compile(rf"(\b(?:mysql|mariadb)[\w-]*\b[^\n]*?\s-p)(?=[^\s-]){VALUE}")


def redact_text(text: str) -> str:
    """Mask password values in a command or traceback before it is logged or sent to Press"""
    for pattern in (
        PASSWORD_FLAG_PATTERN,
        LIST_PASSWORD_FLAG_PATTERN,
        ADMIN_PASSWORD_PATTERN,
        MYSQL_PASSWORD_PATTERN,
    ):
        text = pattern.sub(rf"\g<1>{REDACTED}", text)
    return text


def redact_command(command):
    if isinstance(command, str):
        return redact_text(command)
    if isinstance(command, (list, tuple)):
        return _redact_argv(list(command))
    return command


def _redact_argv(argv: list) -> list:
    # A list command has the flag and its value as separate items
    return [_redact_argv_item(argv[index - 1] if index else None, item) for index, item in enumerate(argv)]


def _redact_argv_item(previous, item):
    if not isinstance(item, str):
        return item
    if isinstance(previous, str) and _takes_password_value(previous):
        return REDACTED
    return redact_text(item)


def _takes_password_value(flag: str) -> bool:
    return flag == "set-admin-password" or bool(
        re.fullmatch(r"--(?![\w-]*-stdin$)[\w-]*password[\w-]*", flag)
    )


def redact_call_arguments(function, args, kwargs) -> tuple[list, dict]:
    """Mask the arguments of a job whose parameter name holds a password or a secret"""
    names = list(inspect.signature(function).parameters)
    args = [
        REDACTED if index < len(names) and _is_secret(names[index]) else arg for index, arg in enumerate(args)
    ]
    kwargs = {name: REDACTED if _is_secret(name) else value for name, value in kwargs.items()}
    return args, kwargs


def _is_secret(name: str) -> bool:
    return "password" in name or "secret" in name
