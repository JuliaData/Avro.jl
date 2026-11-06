"""fastavro reader and writer benchmark.

usage: python -I bench.py read <shape> <file> <iters> [count]
       python -I bench.py write <shape> <infile> <outfile> <codec> <iters>
Prints per iteration `iter <k> <s> <rows> <isum> <nstr> <nbytes> key=value...`. The `count` read
mode only counts records, to measure the checksum's own cost.
"""
import datetime
import gc
import json
import os
import sys
import time
from array import array

import fastavro

M61 = (1 << 61) - 1
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
US = datetime.timedelta(microseconds=1)
KIND = {k: i for i, k in enumerate(["CLICK", "VIEW", "PURCHASE", "SIGNUP", "LOGOUT"])}
LEVELS = {"null": None, "deflate": 6, "snappy": None, "zstandard": 3}
FLUSH = 1 << 16


def dbits(dbl):
    """Sum of the IEEE-754 bit patterns of `dbl`, read as signed 64-bit integers."""
    return sum(array("q", array("d", dbl).tobytes()))


# Strings are ASCII in these files, so len() is the UTF-8 byte length.
def ck_numeric(recs):
    rows = isum = 0
    dbl = []
    for r in recs:
        rows += 1
        isum += r["a"] + r["b"] + r["c"] + r["d"] + r["n"] + r["flag"]
        dbl.append(r["x"])
        dbl.append(r["y"])
        if len(dbl) >= FLUSH:
            isum += dbits(dbl)
            dbl.clear()
    return rows, isum + dbits(dbl), 0, 0


def ck_strings(recs):
    rows = nstr = nbytes = 0
    for r in recs:
        rows += 1
        nstr += len(r)
        for v in r.values():
            nbytes += len(v)
    return rows, 0, nstr, nbytes


def ck_mixed(recs):
    rows = isum = nstr = nbytes = 0
    dbl = []
    for r in recs:
        rows += 1
        isum += r["id"] + (r["ts"] - EPOCH) // US + r["count"] + KIND[r["kind"]] + r["ok"]
        p = r["parent"]
        if p is not None:
            isum += p
        u = r["user"]
        if u is not None:
            nstr += 1
            nbytes += len(u)
        nstr += 2
        nbytes += len(r["name"]) + len(r["tag"])
        dbl.append(r["value"])
        if len(dbl) >= FLUSH:
            isum += dbits(dbl)
            dbl.clear()
    return rows, isum + dbits(dbl), nstr, nbytes


def ck_nested(recs):
    rows = isum = nstr = nbytes = 0
    dbl = []
    for r in recs:
        rows += 1
        attrs = r["attrs"]
        pt = r["point"]
        isum += r["id"] + sum(r["items"]) + sum(attrs.values())
        nstr += len(attrs) + 1
        nbytes += sum(map(len, attrs)) + len(pt["label"])
        dbl.append(pt["w"])
        if len(dbl) >= FLUSH:
            isum += dbits(dbl)
            dbl.clear()
    return rows, isum + dbits(dbl), nstr, nbytes


def ck_count(recs):
    rows = 0
    for _ in recs:
        rows += 1
    return rows, 0, 0, 0


CHECKSUMS = {"numeric": ck_numeric, "strings": ck_strings, "mixed": ck_mixed, "nested": ck_nested}


def read(shape, path, iters, mode="checksum"):
    ck = ck_count if mode == "count" else CHECKSUMS[shape]
    for k in range(1, iters + 1):
        t0 = time.perf_counter()
        with open(path, "rb") as f:
            rows, isum, nstr, nbytes = ck(fastavro.reader(f))
        t1 = time.perf_counter()
        print(f"iter {k} {t1 - t0:.6f} {rows} {isum & M61} {nstr} {nbytes}", flush=True)


def write(shape, infile, outfile, codec, iters):
    with open(os.path.join(os.path.dirname(__file__), "..", "schemas", f"{shape}.avsc")) as f:
        schema = fastavro.parse_schema(json.load(f))
    with open(infile, "rb") as f:
        recs = list(fastavro.reader(f))
    gc.collect()
    gc.freeze()  # the prepared rows are old objects, as in a long-lived process; collections skip them
    for k in range(1, iters + 1):
        t0 = time.perf_counter()
        with open(outfile, "wb") as f:
            fastavro.writer(f, schema, recs, codec=codec, sync_interval=64000,
                            codec_compression_level=LEVELS[codec])
        t1 = time.perf_counter()
        print(f"iter {k} {t1 - t0:.6f} {len(recs)} 0 0 0 out={os.path.getsize(outfile)}", flush=True)


def main(a):
    if a[1] == "read":
        read(a[2], a[3], int(a[4]), *a[5:])
    elif a[1] == "write":
        write(a[2], a[3], a[4], a[5], int(a[6]))
    else:
        raise SystemExit(f"unknown op {a[1]}")


if __name__ == "__main__":
    main(sys.argv)
