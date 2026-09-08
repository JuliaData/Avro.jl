import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;

// Usage: ReadWithReader file.avro [reader.avsc]  -> one JSON-encoded datum per line (Avro JSON encoding)
public class ReadWithReader {
    public static void main(String[] args) throws Exception {
        File f = new File(args[0]);
        GenericDatumReader<Object> dr = new GenericDatumReader<>();
        if (args.length > 1) dr.setExpected(new Schema.Parser().parse(new File(args[1])));
        try (DataFileReader<Object> r = new DataFileReader<>(f, dr)) {
            Schema out = args.length > 1 ? new Schema.Parser().parse(new File(args[1])) : r.getSchema();
            GenericDatumWriter<Object> w = new GenericDatumWriter<>(out);
            while (r.hasNext()) {
                ByteArrayOutputStream bo = new ByteArrayOutputStream();
                JsonEncoder je = EncoderFactory.get().jsonEncoder(out, bo);
                w.write(r.next(), je);
                je.flush();
                System.out.println(bo.toString("UTF-8"));
            }
        }
    }
}
