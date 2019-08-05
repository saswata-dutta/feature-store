# -*- coding: utf-8 -*-

import re

NON_WORD_RE = re.compile(r"\W")
MULTI_US_RE = re.compile(r"_{2,}")


def sanitise(name: str) -> str:
    """
    lower case all alphabets,
    replace all non alphanumeric by underscores,
    remove excess underscores
    :param name:
    :return: sanitised name eligible for a hive table col
    """
    safe_char = "_"
    sanitised = NON_WORD_RE.sub(safe_char, name.strip().lower())
    return MULTI_US_RE.sub(safe_char, sanitised.strip(safe_char))


def stream2str(stream) -> str:
    return stream.read().decode("utf-8")


def strip(s: str, prefix: str, suffix: str) -> str:
    """
    Strip `prefix` and `suffix` from the string
    :param s:
    :param prefix:
    :param suffix:
    :return:
    """
    s1 = _strip_prefix(s, prefix)
    s2 = _strip_suffix(s1, suffix)
    return s2


def _strip_prefix(s: str, prefix: str) -> str:
    return s[len(prefix) :] if s.startswith(prefix) else s  # noqa: E203


def _strip_suffix(s: str, suffix: str) -> str:
    return s[: -len(suffix)] if s.endswith(suffix) else s
