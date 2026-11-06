import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import peerbench.Event;
import peerbench.Nested;
import peerbench.Numeric;
import peerbench.Point;
import peerbench.Strings;

/**
 * Apache Avro Java reader and writer benchmark.
 *
 * <p>usage: Bench read &lt;generic|specific&gt; &lt;shape&gt; &lt;file&gt; &lt;iters&gt;; Bench write
 * &lt;generic|specific&gt; &lt;shape&gt; &lt;infile&gt; &lt;outfile&gt; &lt;codec&gt; &lt;iters&gt;. Prints per
 * iteration {@code iter <k> <s> <rows> <isum> <nstr> <nbytes> key=value...}.
 */
public final class Bench {
  static final long M61 = (1L << 61) - 1;
  static final com.sun.management.ThreadMXBean THREADS =
      (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
  static long rows, isum, nstr, nbytes;

  static void str(CharSequence cs) {
    nstr++;
    nbytes += cs instanceof Utf8 u ? u.getByteLength() : cs.toString().getBytes(StandardCharsets.UTF_8).length;
  }

  static void walk(Object v) {
    if (v == null) return;
    if (v instanceof Long l) isum += l;
    else if (v instanceof Utf8 u) str(u);
    else if (v instanceof Double d) isum += Double.doubleToRawLongBits(d);
    else if (v instanceof Integer i) isum += i;
    else if (v instanceof Boolean b) isum += b ? 1 : 0;
    else if (v instanceof ByteBuffer bb) {
      nstr++;
      nbytes += bb.remaining();
    } else if (v instanceof GenericData.EnumSymbol e) isum += e.getSchema().getEnumOrdinal(e.toString());
    else if (v instanceof IndexedRecord r) {
      int n = r.getSchema().getFields().size();
      for (int i = 0; i < n; i++) walk(r.get(i));
    } else if (v instanceof List<?> a) {
      for (Object o : a) walk(o);
    } else if (v instanceof Map<?, ?> m) {
      for (Map.Entry<?, ?> e : m.entrySet()) {
        walk(e.getKey());
        walk(e.getValue());
      }
    } else throw new IllegalStateException("unexpected " + v.getClass());
  }

  static void ck(Numeric r) {
    isum += r.getA() + r.getB() + r.getC() + r.getD() + r.getN() + (r.getFlag() ? 1 : 0)
        + Double.doubleToRawLongBits(r.getX()) + Double.doubleToRawLongBits(r.getY());
  }

  static void ck(Strings r) {
    str(r.getS0()); str(r.getS1()); str(r.getS2()); str(r.getS3()); str(r.getS4());
    str(r.getS5()); str(r.getS6()); str(r.getS7()); str(r.getS8()); str(r.getS9());
  }

  static void ck(Event r) {
    Instant t = r.getTs();
    isum += r.getId() + t.getEpochSecond() * 1_000_000L + t.getNano() / 1000 + r.getCount()
        + r.getKind().ordinal() + (r.getOk() ? 1 : 0) + Double.doubleToRawLongBits(r.getValue());
    Long p = r.getParent();
    if (p != null) isum += p;
    str(r.getName());
    CharSequence u = r.getUser();
    if (u != null) str(u);
    nstr++;
    nbytes += r.getTag().remaining();
  }

  static void ck(Nested r) {
    isum += r.getId();
    for (Long x : r.getItems()) isum += x;
    for (Map.Entry<CharSequence, Long> e : r.getAttrs().entrySet()) {
      str(e.getKey());
      isum += e.getValue();
    }
    Point p = r.getPoint();
    str(p.getLabel());
    isum += Double.doubleToRawLongBits(p.getW());
  }

  static <T> void readFile(File f, DatumReader<T> dr, Consumer<T> ck) throws Exception {
    try (DataFileReader<T> r = new DataFileReader<>(f, dr)) {
      T rec = null;
      while (r.hasNext()) {
        rec = r.next(rec);
        rows++;
        ck.accept(rec);
      }
    }
  }

  static void readOnce(String api, String shape, File f) throws Exception {
    if (api.equals("generic")) {
      readFile(f, new GenericDatumReader<GenericRecord>(), Bench::walk);
      return;
    }
    switch (shape) {
      case "numeric" -> readFile(f, new SpecificDatumReader<>(Numeric.class), (Numeric r) -> ck(r));
      case "strings" -> readFile(f, new SpecificDatumReader<>(Strings.class), (Strings r) -> ck(r));
      case "mixed" -> readFile(f, new SpecificDatumReader<>(Event.class), (Event r) -> ck(r));
      case "nested" -> readFile(f, new SpecificDatumReader<>(Nested.class), (Nested r) -> ck(r));
      default -> throw new IllegalArgumentException(shape);
    }
  }

  static Class<?> specificClass(String shape) {
    return switch (shape) {
      case "numeric" -> Numeric.class;
      case "strings" -> Strings.class;
      case "mixed" -> Event.class;
      case "nested" -> Nested.class;
      default -> throw new IllegalArgumentException(shape);
    };
  }

  static CodecFactory codec(String name) {
    return switch (name) {
      case "null" -> CodecFactory.nullCodec();
      case "deflate" -> CodecFactory.deflateCodec(6);
      case "snappy" -> CodecFactory.snappyCodec();
      case "zstandard" -> CodecFactory.zstandardCodec(3);
      default -> throw new IllegalArgumentException(name);
    };
  }

  static long gcMillis() {
    long t = 0;
    for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) t += Math.max(0, b.getCollectionTime());
    return t;
  }

  @SuppressWarnings("unchecked")
  static void write(String api, String shape, File in, File out, String codecName, int iters) throws Exception {
    boolean generic = api.equals("generic");
    Class<?> cls = generic ? null : specificClass(shape);
    DatumReader<Object> dr = generic ? new GenericDatumReader<>() : new SpecificDatumReader<>((Class<Object>) cls);
    List<Object> recs = new ArrayList<>();
    Schema schema;
    try (DataFileReader<Object> r = new DataFileReader<>(in, dr)) {
      schema = r.getSchema();
      while (r.hasNext()) recs.add(r.next());
    }
    System.gc();
    for (int k = 1; k <= iters; k++) {
      long g0 = gcMillis(), m0 = THREADS.getCurrentThreadAllocatedBytes(), t0 = System.nanoTime();
      DatumWriter<Object> dw = generic ? new GenericDatumWriter<>(schema) : new SpecificDatumWriter<>((Class<Object>) cls);
      try (DataFileWriter<Object> w = new DataFileWriter<>(dw)) {
        w.setCodec(codec(codecName));
        w.create(schema, out);
        for (Object o : recs) w.append(o);
      }
      long t1 = System.nanoTime(), g1 = gcMillis(), m1 = THREADS.getCurrentThreadAllocatedBytes();
      System.out.println(String.format(Locale.ROOT, "iter %d %.6f %d 0 0 0 gc=%.3f alloc=%d out=%d",
          k, (t1 - t0) / 1e9, recs.size(), (g1 - g0) / 1e3, m1 - m0, out.length()));
    }
  }

  public static void main(String[] a) throws Exception {
    if (a[0].equals("read")) {
      File f = new File(a[3]);
      int iters = Integer.parseInt(a[4]);
      for (int k = 1; k <= iters; k++) {
        rows = isum = nstr = nbytes = 0;
        long g0 = gcMillis(), m0 = THREADS.getCurrentThreadAllocatedBytes(), t0 = System.nanoTime();
        readOnce(a[1], a[2], f);
        long t1 = System.nanoTime(), g1 = gcMillis(), m1 = THREADS.getCurrentThreadAllocatedBytes();
        System.out.println(String.format(Locale.ROOT, "iter %d %.6f %d %d %d %d gc=%.3f alloc=%d",
            k, (t1 - t0) / 1e9, rows, isum & M61, nstr, nbytes, (g1 - g0) / 1e3, m1 - m0));
      }
    } else if (a[0].equals("write")) {
      write(a[1], a[2], new File(a[3]), new File(a[4]), a[5], Integer.parseInt(a[6]));
    } else {
      throw new IllegalArgumentException(a[0]);
    }
  }
}
