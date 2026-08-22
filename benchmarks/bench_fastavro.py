# Baseline for fastavro (run inside the pinned venv). Reads/writes the same 1M-row, 4-column shape.
import time, sys, os, fastavro, random
random.seed(1)
N = 1_000_000
d = sys.argv[1]
recs = [{"id": i + 1, "x": random.random(), "name": "name_%d" % random.randint(1000, 9999), "flag": random.random() < 0.5} for i in range(N)]
schema = {"type":"record","name":"Bench","namespace":"avro.jl.test","fields":[{"name":"id","type":"long"},{"name":"x","type":"double"},{"name":"name","type":"string"},{"name":"flag","type":"boolean"}]}
print(f"impl=fastavro version={fastavro.__version__} python={sys.version.split()[0]} rows={N}")
for codec in ("null", "zstandard", "deflate"):
    f = os.path.join(d, f"benchfa-{codec}.avro")
    ts = []
    for _ in range(3):
        t = time.perf_counter()
        with open(f, "wb") as fo: fastavro.writer(fo, schema, recs, codec=codec)
        ts.append(time.perf_counter() - t)
    print(f"metric=write codec={codec} best_s={min(ts):.4f} all_s={ts} bytes={os.path.getsize(f)}")
for codec in ("null", "zstandard"):
    f = os.path.join(d, f"benchfa-{codec}.avro"); ts = []
    for _ in range(3):
        t = time.perf_counter()
        with open(f, "rb") as fo: n = sum(1 for _ in fastavro.reader(fo))
        ts.append(time.perf_counter() - t)
    print(f"metric=read_to_dicts codec={codec} best_s={min(ts):.4f} all_s={ts} rows={n}")
