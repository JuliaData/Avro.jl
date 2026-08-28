# Live interop fixtures

These fixtures are the small, reviewable inputs for the live Avro 1.12.2 differential matrix in
`test/interop.jl`.

- `collections/` holds positive-count and negative-count/sized encodings for both arrays and maps.
- `negative/` holds malformed schema, datum, and JSON inputs. `verdicts.tsv` records the expected
  Julia, Java, and fastavro verdict for every negative case and the dynamically mutated container
  cases.

The verdicts were measured with avro-tools 1.12.2 and fastavro 1.12.2. Fastavro accepts duplicate
record fields and invalid Avro names while Java and Avro.jl reject them. Java tolerates a truncated
final sync marker and non-domain boolean bytes. Fastavro tolerates non-domain boolean bytes and an
invalid first magic byte. These are recorded oracle differences, not Avro.jl acceptance targets.
