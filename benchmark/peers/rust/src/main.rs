//! apache-avro (`Value` rows) and arrow-avro (`RecordBatch` columns) benchmark.
//!
//! usage: peerbench read <value|arrow> <file> <iters> [batch_rows]
//!        peerbench write <value|arrow> <shape> <infile> <outfile> <codec> <iters> [batch_rows]
//! Prints per iteration `iter <k> <s> <rows> <isum> <nstr> <nbytes> key=value...`.
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::time::Instant;

use apache_avro::types::Value;
use apache_avro::{Codec, DeflateSettings, Reader, Writer, ZstandardSettings};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch};
use arrow_avro::compression::CompressionCodec;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::writer::format::AvroOcfFormat;
use arrow_avro::writer::WriterBuilder;
use arrow_schema::{DataType, TimeUnit};

const M61: i64 = (1 << 61) - 1;
const BUF: usize = 1 << 20; // file buffer: the 8 KiB default spends 10-15% of a read in read(2)
const KINDS: [&str; 5] = ["CLICK", "VIEW", "PURCHASE", "SIGNUP", "LOGOUT"];

#[derive(Default)]
struct Ck {
    rows: i64,
    isum: i64,
    nstr: i64,
    nbytes: i64,
}

impl Ck {
    fn add(&mut self, x: i64) {
        self.isum = self.isum.wrapping_add(x);
    }
    fn s(&mut self, n: usize) {
        self.nstr += 1;
        self.nbytes += n as i64;
    }
}

fn walk(v: &Value, c: &mut Ck) {
    match v {
        Value::Null => {}
        Value::Boolean(b) => c.add(*b as i64),
        Value::Int(i) => c.add(*i as i64),
        Value::Long(l) | Value::TimestampMicros(l) => c.add(*l),
        Value::Double(d) => c.add(d.to_bits() as i64),
        Value::String(s) => c.s(s.len()),
        Value::Bytes(b) => c.s(b.len()),
        Value::Enum(i, _) => c.add(*i as i64),
        Value::Union(_, b) => walk(b, c),
        Value::Array(a) => a.iter().for_each(|x| walk(x, c)),
        Value::Map(m) => m.iter().for_each(|(k, x)| {
            c.s(k.len());
            walk(x, c)
        }),
        Value::Record(fs) => fs.iter().for_each(|(_, x)| walk(x, c)),
        other => panic!("unexpected value {other:?}"),
    }
}

fn read_value(path: &str) -> Ck {
    let mut c = Ck::default();
    let r = Reader::new(BufReader::with_capacity(BUF, File::open(path).unwrap())).unwrap();
    for v in r {
        c.rows += 1;
        walk(&v.unwrap(), &mut c);
    }
    c
}

fn add_valid(c: &mut Ck, a: &dyn Array, it: impl Iterator<Item = i64>) {
    if a.null_count() == 0 {
        it.for_each(|x| c.add(x));
    } else {
        it.enumerate().filter(|(i, _)| a.is_valid(*i)).for_each(|(_, x)| c.add(x));
    }
}

fn add_lens(c: &mut Ck, a: &dyn Array, o: &[i32]) {
    if a.null_count() == 0 {
        c.nstr += a.len() as i64;
        c.nbytes += (o[a.len()] - o[0]) as i64;
    } else {
        (0..a.len()).filter(|&i| a.is_valid(i)).for_each(|i| c.s((o[i + 1] - o[i]) as usize));
    }
}

fn walk_array(a: &dyn Array, c: &mut Ck) {
    match a.data_type() {
        DataType::Int64 => add_valid(c, a, a.as_primitive::<Int64Type>().values().iter().copied()),
        DataType::Int32 => add_valid(c, a, a.as_primitive::<Int32Type>().values().iter().map(|&x| x as i64)),
        DataType::Float64 => {
            add_valid(c, a, a.as_primitive::<Float64Type>().values().iter().map(|x| x.to_bits() as i64))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            add_valid(c, a, a.as_primitive::<TimestampMicrosecondType>().values().iter().copied())
        }
        DataType::Boolean => c.add(a.as_boolean().true_count() as i64),
        DataType::Utf8 => add_lens(c, a, a.as_string::<i32>().value_offsets()),
        DataType::Binary => add_lens(c, a, a.as_binary::<i32>().value_offsets()),
        DataType::Dictionary(_, _) => {
            // ordinals by symbol name, whatever order the dictionary holds them in
            let d = a.as_dictionary::<Int32Type>();
            let vals = d.values().as_string::<i32>();
            let ord: Vec<i64> =
                (0..vals.len()).map(|i| KINDS.iter().position(|k| *k == vals.value(i)).unwrap() as i64).collect();
            add_valid(c, a, d.keys().values().iter().map(|&k| ord[k as usize]))
        }
        DataType::List(_) => {
            let l = a.as_list::<i32>();
            let o = l.value_offsets();
            walk_array(&l.values().slice(o[0] as usize, (o[o.len() - 1] - o[0]) as usize), c)
        }
        DataType::Map(_, _) => {
            let m = a.as_map();
            let o = m.value_offsets();
            let (s, n) = (o[0] as usize, (o[o.len() - 1] - o[0]) as usize);
            walk_array(&m.keys().slice(s, n), c);
            walk_array(&m.values().slice(s, n), c);
        }
        DataType::Struct(_) => a.as_struct().columns().iter().for_each(|col| walk_array(col, c)),
        t => panic!("unexpected arrow type {t}"),
    }
}

fn arrow_reader(path: &str, batch: usize) -> arrow_avro::reader::Reader<BufReader<File>> {
    ReaderBuilder::new().with_batch_size(batch).build(BufReader::with_capacity(BUF, File::open(path).unwrap())).unwrap()
}

fn read_arrow(path: &str, batch: usize) -> Ck {
    let mut c = Ck::default();
    for b in arrow_reader(path, batch) {
        let b = b.unwrap();
        c.rows += b.num_rows() as i64;
        b.columns().iter().for_each(|col| walk_array(col, &mut c));
    }
    c
}

fn value_codec(name: &str) -> Codec {
    match name {
        "null" => Codec::Null,
        "deflate" => Codec::Deflate(DeflateSettings::default()),
        "snappy" => Codec::Snappy,
        "zstandard" => Codec::Zstandard(ZstandardSettings::new(3)),
        _ => panic!("unknown codec {name}"),
    }
}

fn arrow_codec(name: &str) -> Option<CompressionCodec> {
    match name {
        "null" => None,
        "deflate" => Some(CompressionCodec::Deflate),
        "snappy" => Some(CompressionCodec::Snappy),
        "zstandard" => Some(CompressionCodec::ZStandard),
        _ => panic!("unknown codec {name}"),
    }
}

fn write_value(infile: &str, out: &str, codec: &str) -> Box<dyn FnMut() -> usize> {
    let r = Reader::new(BufReader::with_capacity(BUF, File::open(infile).unwrap())).unwrap();
    let schema = r.writer_schema().clone();
    let vals: Vec<Value> = r.map(|v| v.unwrap()).collect();
    let (out, codec) = (out.to_string(), value_codec(codec));
    Box::new(move || {
        let sink = BufWriter::with_capacity(BUF, File::create(&out).unwrap());
        let mut w = Writer::builder().schema(&schema).writer(sink).codec(codec).block_size(64000).build().unwrap();
        vals.iter().for_each(|v| {
            w.append_value_ref(v).unwrap();
        });
        w.into_inner().unwrap().flush().unwrap();
        vals.len()
    })
}

fn write_arrow(shape: &str, infile: &str, out: &str, codec: &str, batch: usize) -> Box<dyn FnMut() -> usize> {
    let batches: Vec<RecordBatch> = arrow_reader(infile, batch).map(|b| b.unwrap()).collect();
    let avsc = std::fs::read_to_string(format!("{}/../schemas/{shape}.avsc", env!("CARGO_MANIFEST_DIR"))).unwrap();
    let base = batches[0].schema();
    let mut md = base.metadata().clone();
    md.insert("avro.schema".to_string(), avsc); // write the original Avro schema verbatim
    let schema = arrow_schema::Schema::new_with_metadata(base.fields().clone(), md);
    let (out, codec) = (out.to_string(), arrow_codec(codec));
    Box::new(move || {
        let sink = BufWriter::with_capacity(BUF, File::create(&out).unwrap());
        let mut w = WriterBuilder::new(schema.clone()).with_compression(codec).build::<_, AvroOcfFormat>(sink).unwrap();
        batches.iter().for_each(|b| w.write(b).unwrap());
        w.finish().unwrap();
        w.into_inner().flush().unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "read" => {
            let (mode, path, iters) = (a[2].as_str(), a[3].as_str(), a[4].parse::<usize>().unwrap());
            let batch = a.get(5).map_or(8192, |s| s.parse().unwrap());
            for k in 1..=iters {
                let t0 = Instant::now();
                let c = if mode == "value" { read_value(path) } else { read_arrow(path, batch) };
                let dt = t0.elapsed().as_secs_f64();
                println!("iter {k} {dt:.6} {} {} {} {}", c.rows, c.isum & M61, c.nstr, c.nbytes);
            }
        }
        "write" => {
            let (mode, shape, infile, out, codec) = (a[2].as_str(), a[3].as_str(), a[4].as_str(), a[5].as_str(), a[6].as_str());
            let iters = a[7].parse::<usize>().unwrap();
            let batch = a.get(8).map_or(8192, |s| s.parse().unwrap());
            let mut run = if mode == "value" { write_value(infile, out, codec) } else { write_arrow(shape, infile, out, codec, batch) };
            for k in 1..=iters {
                let t0 = Instant::now();
                let n = run();
                let dt = t0.elapsed().as_secs_f64();
                println!("iter {k} {dt:.6} {n} 0 0 0 out={}", std::fs::metadata(out).unwrap().len());
            }
        }
        op => panic!("unknown op {op}"),
    }
}
