#!/usr/bin/env bash
# Regenerates test/fixtures/generated from the schema files. Requires: java (JDK 17+), the pinned avro-tools jar
# (AVRO_TOOLS_JAR, sha256 6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68), a Python with fastavro==1.12.2 + cramjam (PYTHON).
set -euo pipefail
shopt -s nullglob
here="$(cd "$(dirname "$0")" && pwd)"
fixtures="$here"
gen="$here/generated"
JAR="${AVRO_TOOLS_JAR:?set AVRO_TOOLS_JAR}"; PY="${PYTHON:-python3}"
J() { java -jar "$JAR" "$@"; }
echo "$(shasum -a 256 "$JAR")"
# Build the Java harness (test/interop/java) into a scratch classpath.
HOUT="$(mktemp -d)"
javac -cp "$JAR" -d "$HOUT" "$here"/../interop/java/*.java
CP="$JAR:$HOUT"
mkdir -p "$gen/data" "$gen/evolution" "$gen/singleobject" "$gen/blocking" "$gen/canonical" "$gen/roots"
here="$gen"
# Derived fastavro files are replaced as one set. This removes stale outputs from an interrupted or
# older non-idempotent generator before any base `*-null.avro` glob is evaluated.
for f in "$here"/{data,roots}/*-fastavro-*.avro; do rm -- "$f"; done
# Non-record roots: every kind, random data + tojson expectations.
for r in "$gen"/roots/*.avsc; do b="$(basename "$r" .avsc)"
  J random --count 20 --seed 11 --schema-file "$r" "$gen/roots/$b-null.avro" 2>/dev/null
  J tojson "$gen/roots/$b-null.avro" > "$gen/roots/$b.jsonl"
done
for s in bench everything wide interop; do J random --count 50 --seed 7 --schema-file "$here/schemas/$s.avsc" "$here/data/$s-null.avro"; done
J random --count 3 --seed 7 --schema-file "$here/schemas/empty.avsc" "$here/data/empty-null.avro"
J fromjson --schema-file "$here/schemas/logical.avsc" "$here/logical.json" > "$here/data/logical-null.avro"
for f in "$here"/{data,roots}/*-null.avro; do b=$(basename "$f" -null.avro); dir=$(dirname "$f")
  for c in deflate snappy bzip2 zstandard; do J recodec --codec $c "$f" "$dir/$b-$c.avro"; done
  J recodec --codec xz --level 6 "$f" "$dir/$b-xz.avro"
  J tojson "$f" > "$dir/$b.jsonl"
done
"$PY" - "$here" <<'PYEOF'
import fastavro, glob, os, sys
here = sys.argv[1]
bases = sorted(
    f
    for directory in ("data", "roots")
    for f in glob.glob(os.path.join(here, directory, "*-null.avro"))
    if "-fastavro-" not in os.path.basename(f)
)
codecs = ("null", "deflate", "bzip2", "snappy", "zstandard", "xz")
for f in bases:
    b = f[:-len("-null.avro")]
    with open(f, "rb") as fh:
        r = fastavro.reader(fh); recs = list(r); sch = r.writer_schema
    for codec in codecs:
        try:
            with open(f"{b}-fastavro-{codec}.avro", "wb") as out: fastavro.writer(out, sch, recs, codec=codec)
        except ValueError as e:
            print(f"fastavro {codec}: {e}", file=sys.stderr)
for codec in codecs:
    outputs = [
        f
        for directory in ("data", "roots")
        for f in glob.glob(os.path.join(here, directory, f"*-fastavro-{codec}.avro"))
    ]
    if len(outputs) != len(bases):
        raise RuntimeError(f"fastavro {codec}: generated {len(outputs)} files for {len(bases)} inputs")
if any(glob.glob(os.path.join(here, directory, "*-fastavro-fastavro-*.avro")) for directory in ("data", "roots")):
    raise RuntimeError("nested fastavro outputs remain after regeneration")
PYEOF
for r in everything_readerA everything_readerB; do java -cp "$CP" ReadWithReader "$here/data/everything-null.avro" "$here/evolution/$r.avsc" > "$here/evolution/$r.jsonl"; done
java -cp "$CP" ReadWithReader "$fixtures/apache/weather.avro" "$here/evolution/weather_reader.avsc" > "$here/evolution/weather_reader.jsonl"
java -cp "$CP" SingleObject encode "$here/singleobject/weather.avsc" "$here/singleobject/weather1.json" > "$here/singleobject/weather1.bin"
for bs in 32 64 1024; do java -cp "$CP" BlockingEncode "$here/blocking/arrmap.avsc" "$here/blocking/arrmap.json" $bs > "$here/blocking/arrmap-$bs.bin"; done
: > "$here/fingerprints.tsv"
for f in "$here"/schemas/*.avsc "$here"/evolution/*.avsc "$fixtures"/apache/*.avsc; do b=$(basename "$f")
  J canonical "$f" "$here/canonical/$b.canonical"
  for alg in CRC-64-AVRO MD5 SHA-256; do printf '%s\t%s\t%s\n' "$b" "$alg" "$(J fingerprint --fingerprint $alg "$f" | tail -1 | awk '{print $1}')" >> "$here/fingerprints.tsv"; done
done
java -cp "$CP" BigDec 12.345 -1.5 0 123456789012345678901234567890.5 > "$here/bigdecimal.tsv"
# Not regenerated here (documented inputs): legacy1x (written by the pinned Avro.jl 1.1.2 in its own
# environment: julia +1.10 --project=<avro112 env> legacy1x/write_legacy.jl), highwindow (zstd CLI
# --window-log=30 / xz --lzma2=dict=1GiB payloads wrapped by recodec), sortorder (CompareBytes over
# sortorder/cases.jsonl -> verdicts.tsv).
echo "done"
