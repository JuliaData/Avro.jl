import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.message.*;
import java.io.*;
import java.nio.ByteBuffer;

// Usage: SingleObject encode schema.avsc datum.json > out.bin | SingleObject decode schema.avsc in.bin -> JSON
public class SingleObject {
    public static void main(String[] args) throws Exception {
        Schema s = new Schema.Parser().parse(new File(args[1]));
        if (args[0].equals("encode")) {
            String json = new String(java.nio.file.Files.readAllBytes(new File(args[2]).toPath()), "UTF-8");
            Decoder d = DecoderFactory.get().jsonDecoder(s, json);
            Object datum = new GenericDatumReader<Object>(s).read(null, d);
            BinaryMessageEncoder<Object> enc = new BinaryMessageEncoder<>(GenericData.get(), s);
            ByteBuffer bb = enc.encode(datum);
            byte[] b = new byte[bb.remaining()]; bb.get(b);
            System.out.write(b); System.out.flush();
        } else {
            byte[] b = java.nio.file.Files.readAllBytes(new File(args[2]).toPath());
            BinaryMessageDecoder<Object> dec = new BinaryMessageDecoder<>(GenericData.get(), s);
            Object datum = dec.decode(b);
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            JsonEncoder je = EncoderFactory.get().jsonEncoder(s, bo);
            new GenericDatumWriter<Object>(s).write(datum, je); je.flush();
            System.out.println(bo.toString("UTF-8"));
        }
    }
}
