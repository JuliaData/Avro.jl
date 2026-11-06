"""Write one deterministic benchmark container file.

usage: python -I gen.py <schemas-dir> <shape> <rows> <codec> <out>

The rows depend only on the shape's seed and the row count, so every codec of a shape holds the
same datums, and a smaller row count yields a prefix of a larger one. Strings are ASCII, so a
string's character count equals its UTF-8 byte count. Blocks are flushed at 64,000 bytes, the
Java implementation's default sync interval.
"""
import json
import os
import random
import sys

import fastavro

SEEDS = {"numeric": 1, "strings": 2, "mixed": 3, "nested": 4}
ALNUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
KINDS = ["CLICK", "VIEW", "PURCHASE", "SIGNUP", "LOGOUT"]
LEVELS = {"null": None, "deflate": 6, "snappy": None, "zstandard": 3}


def rows(shape, n):
    rng = random.Random(SEEDS[shape])
    pool = "".join(rng.choices(ALNUM, k=1 << 20))
    top = len(pool) - 64

    def s(lo, hi):
        o = rng.randrange(top)
        return pool[o:o + rng.randint(lo, hi)]

    if shape == "numeric":
        for i in range(n):
            yield {"a": i, "b": rng.randrange(-2**31, 2**31), "c": rng.randrange(1000),
                   "d": rng.getrandbits(64) - 2**63, "x": rng.random() * 1000, "y": rng.gauss(0, 1),
                   "n": rng.randrange(-2**31, 2**31), "flag": rng.random() < 0.5}
    elif shape == "strings":
        for _ in range(n):
            yield {f"s{j}": s(5, 40) for j in range(10)}
    elif shape == "mixed":
        t0 = 1_700_000_000_000_000
        for i in range(n):
            yield {"id": i, "ts": t0 + i * 1000 + rng.randrange(1000), "name": s(5, 20),
                   "user": None if rng.random() < 0.3 else s(8, 16), "value": rng.random() * 100,
                   "count": rng.randrange(1000), "kind": KINDS[rng.randrange(5)],
                   "parent": None if rng.random() < 0.5 else rng.randrange(i + 1),
                   "ok": rng.random() < 0.9, "tag": rng.randbytes(16)}
    elif shape == "nested":
        for i in range(n):
            yield {"id": i, "items": [rng.randrange(-10**6, 10**6) for _ in range(rng.randrange(11))],
                   # the index prefix keeps each map's keys distinct
                   "attrs": {f"{j}{s(3, 8)}": rng.randrange(10**5) for j in range(rng.randrange(6))},
                   "point": {"label": s(5, 15), "w": rng.random()}}
    else:
        raise SystemExit(f"unknown shape {shape}")


def main():
    schemas, shape, n, codec, out = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4], sys.argv[5]
    with open(os.path.join(schemas, f"{shape}.avsc")) as f:
        schema = fastavro.parse_schema(json.load(f))
    tmp = out + ".tmp"
    with open(tmp, "wb") as f:
        fastavro.writer(f, schema, rows(shape, n), codec=codec, sync_interval=64000,
                        codec_compression_level=LEVELS[codec])
    os.replace(tmp, out)


if __name__ == "__main__":
    main()
