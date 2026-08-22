#!/usr/bin/env bash
# Regenerates test/fixtures/generated from the schema files. Requires: java (JDK 17+), the pinned avro-tools jar
# (AVRO_TOOLS_JAR, sha256 6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68), a Python with fastavro==1.12.2 + cramjam (PYTHON).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
JAR="${AVRO_TOOLS_JAR:?set AVRO_TOOLS_JAR}"; PY="${PYTHON:-python3}"; JH="${JAVA_HARNESS:-$here/../../interop/java}"
J() { java -jar "$JAR" "$@"; }
echo "$(shasum -a 256 "$JAR")"
mkdir -p "$here/data" "$here/evolution" "$here/singleobject" "$here/blocking" "$here/canonical"
for s in bench everything wide interop; do J random --count 50 --seed 7 --schema-file "$here/schemas/$s.avsc" "$here/data/$s-null.avro"; done
J random --count 3 --seed 7 --schema-file "$here/schemas/empty.avsc" "$here/data/empty-null.avro"
J fromjson --schema-file "$here/schemas/logical.avsc" "$here/logical.json" > "$here/data/logical-null.avro"
for f in "$here"/data/*-null.avro; do b=$(basename "$f" -null.avro)
  for c in deflate snappy bzip2 zstandard; do J recodec --codec $c "$f" "$here/data/$b-$c.avro"; done
  J recodec --codec xz --level 6 "$f" "$here/data/$b-xz.avro"
  J tojson "$f" > "$here/data/$b.jsonl"
done
"$PY" - "$here" <<'PYEOF'
import fastavro, glob, os, sys
here = sys.argv[1]
for f in sorted(glob.glob(os.path.join(here, "data", "*-null.avro"))):
    b = f[:-len("-null.avro")]
    with open(f, "rb") as fh:
        r = fastavro.reader(fh); recs = list(r); sch = r.writer_schema
    for codec in ("deflate", "xz"):
        with open(f"{b}-fastavro-{codec}.avro", "wb") as out: fastavro.writer(out, sch, recs, codec=codec)
PYEOF
CP="$JAR:$JH/out"
for r in everything_readerA everything_readerB; do java -cp "$CP" ReadWithReader "$here/data/everything-null.avro" "$here/evolution/$r.avsc" > "$here/evolution/$r.jsonl"; done
java -cp "$CP" ReadWithReader "$here/apache/weather.avro" "$here/evolution/weather_reader.avsc" > "$here/evolution/weather_reader.jsonl"
java -cp "$CP" SingleObject encode "$here/singleobject/weather.avsc" "$here/singleobject/weather1.json" > "$here/singleobject/weather1.bin"
for bs in 32 64 1024; do java -cp "$CP" BlockingEncode "$here/blocking/arrmap.avsc" "$here/blocking/arrmap.json" $bs > "$here/blocking/arrmap-$bs.bin"; done
: > "$here/fingerprints.tsv"
for f in "$here"/schemas/*.avsc "$here"/evolution/*.avsc "$here"/apache/*.avsc; do b=$(basename "$f")
  J canonical "$f" "$here/canonical/$b.canonical"
  for alg in CRC-64-AVRO MD5 SHA-256; do printf '%s\t%s\t%s\n' "$b" "$alg" "$(J fingerprint --fingerprint $alg "$f" | tail -1 | awk '{print $1}')" >> "$here/fingerprints.tsv"; done
done
java -cp "$CP" BigDec 12.345 -1.5 0 123456789012345678901234567890.5 > "$here/bigdecimal.tsv"
echo "done"
