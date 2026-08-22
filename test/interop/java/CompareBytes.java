import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;
// Usage: CompareBytes schema.avsc a.json b.json -> encodes both datums to binary and compares with BinaryData.compare (no deserialization)
public class CompareBytes {
    static byte[] enc(Schema s, String json) throws Exception {
        Object d = new GenericDatumReader<Object>(s).read(null, DecoderFactory.get().jsonDecoder(s, json));
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        BinaryEncoder e = EncoderFactory.get().binaryEncoder(bo, null);
        new GenericDatumWriter<Object>(s).write(d, e); e.flush();
        return bo.toByteArray();
    }
    public static void main(String[] args) throws Exception {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        byte[] a = enc(s, new String(java.nio.file.Files.readAllBytes(new File(args[1]).toPath()), "UTF-8"));
        byte[] b = enc(s, new String(java.nio.file.Files.readAllBytes(new File(args[2]).toPath()), "UTF-8"));
        System.out.println(Integer.signum(org.apache.avro.io.BinaryData.compare(a, 0, b, 0, s)));
    }
}
