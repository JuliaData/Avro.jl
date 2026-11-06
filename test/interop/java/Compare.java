import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;

// Usage: Compare schema.avsc a.json b.json -> prints -1/0/1 per Avro sort order
public class Compare {
    public static void main(String[] args) throws Exception {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        GenericDatumReader<Object> r = new GenericDatumReader<>(s);
        Object a = r.read(null, DecoderFactory.get().jsonDecoder(s, new String(java.nio.file.Files.readAllBytes(new File(args[1]).toPath()), "UTF-8")));
        Object b = r.read(null, DecoderFactory.get().jsonDecoder(s, new String(java.nio.file.Files.readAllBytes(new File(args[2]).toPath()), "UTF-8")));
        System.out.println(Integer.signum(GenericData.get().compare(a, b, s)));
    }
}
