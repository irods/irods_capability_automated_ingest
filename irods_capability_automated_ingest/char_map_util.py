import base64
import hashlib
import logging
import re
import collections
import string
import functools

__all__ = ["translate_path"]
_regex = re.compile("")

# use as a key in the character mapping to ensure a regex matches to exactly one character


def _re_wrapper(regex):
    return lambda ch: type(ch) is str and len(ch) == 1 and regex.match(ch)


_string_replace = lambda _string, _map: _string.translate(
    {ord(k): v for k, v in _map.items() if v is not None}
)

_logger = logging.getLogger("char_map_util")

SEPARATORS = [c for c in "-~_"]
Allowed = [
    {},
    {
        "separators": "".join(SEPARATORS),
        "radixchars": string.digits + string.ascii_letters,
        "punctuation": "".join(
            sorted(set(string.punctuation) - set(["/"] + SEPARATORS))
        ),
    },
]


def _allowed_in_string(s, map_fn):
    s_new = translate_string(s, map_fn)
    return "".join(a for a, b in zip(s, s_new) if a == b)


class InvalidUsage(Exception):
    pass


def _update_Allowed(map_fn=None):
    if len(Allowed) == 2:
        if map_fn is None:
            raise InvalidUsage(
                "The first call to this function needs a dictionary in map_fn"
            )
        d = Allowed.pop()
        Allowed[0].update((k, _allowed_in_string(v, map_fn)) for k, v in d.items())
    return Allowed[0]


def _allowed_of_type(key, map_fn=None):
    return _update_Allowed(map_fn)[key]


_fb_hash = hashlib.sha224
_fb_obj = _fb_hash().digest()


def _fallback(name=None):
    if name is None:
        return _fb_obj
    else:
        h = _fb_hash()
        h.update(name.encode("utf8"))
        return h.digest()


_change_encoding_test = lambda c: c
_change_encoding_default = lambda c: (
    chr(c).encode("utf8") if type(c) is int else c.encode("utf8")
)


# must be called after first use of _encoded_differences()
def _diffs_encoded_to_suffix(diff_bytes, rxarray=None):
    if not diff_bytes:
        return ""
    number = functools.reduce((lambda a, b: (a << 8) | b), diff_bytes)
    radixrep = _update_Allowed()["separators"][:1]
    if rxarray is None:
        rxarray = _update_Allowed()["radixchars"]
    L = len(rxarray)
    while number:
        number, mod = divmod(number, L)
        radixrep += rxarray[mod]
    return radixrep


def translate_string(s, mp):
    if not isinstance(mp, dict):
        mp = collections.OrderedDict(mp)
    for key, value in mp.items():
        if isinstance(key, tuple):
            s = _string_replace(s, {k: value for k in key})
        elif isinstance(key, _regex.__class__):
            s = key.sub(value, s)
        elif isinstance(key, str):
            s = _string_replace(s, {key: value})
        elif callable(key):
            s = "".join(value if key(c) else c for c in s)
    return s


def _encoded_differences(filename, MapFn=None, xfunc=_change_encoding_default):
    rx = _allowed_of_type("radixchars", map_fn=MapFn)
    newname = translate_string(filename, MapFn)
    gen = (
        (tuple(xfunc(_) for _ in a), b)
        for a, b in zip(enumerate(filename), newname)
        if a[1] != b
    )
    MaxBytes = len(_fallback())
    encoded_change = b""
    if xfunc is _change_encoding_test:
        return list(gen)
    # Generate suffix from encoded changes or the constant length SHA2 digest, whichever is shorter.
    while True:
        try:
            g = next(gen)
        except StopIteration:
            break
        encoded_change += b"".join(g[0])
        if len(encoded_change) >= MaxBytes:
            _logger.warning("Using SHA2 for {filename=}")
            return newname, _fallback(filename)
    return newname, encoded_change


def translate_path_element(filename, map_fn, use_suffix=True):
    newname, enc_diffs = _encoded_differences(filename, map_fn)
    if use_suffix:
        suffix = _diffs_encoded_to_suffix(enc_diffs)
        return newname + suffix
    else:
        return newname


def translate_path(path, mp, translate_function=translate_path_element):
    t_elem = []
    for el in path.split("/"):
        if el == "":
            if not t_elem:
                t_elem.append("")
            continue
        new_el = translate_function(el, mp)
        t_elem.append(new_el)
    return "/".join(t_elem)


if __name__ == "__main__":
    # Demonstration
    map_fn = (
        [("!", "~", "0", "1"), "_"],  # map your choice of things to an underscore
        [re.compile("[\u0100-\U00101fff]"), "~"],
    )  # map all non-ascii unicode to a tilde
    m = _update_Allowed(map_fn)
    import pprint

    pprint.pprint(m)
    newname, enc_diffs = _encoded_differences(
        "#041!2~93#041!2\u00ff9\U00101010Z", map_fn
    )
    suffix = _diffs_encoded_to_suffix(enc_diffs)
    print(f"newname={newname}\nsuffix={suffix}")
