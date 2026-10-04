import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;

// Usage: BlockingEncode schema.avsc datum.json blockBufferSize > out.bin   (binary datum using BlockingBinaryEncoder: sized array/map blocks)
public class BlockingEncode {
    public static void main(String[] args) throws Exception {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        String json = new String(java.nio.file.Files.readAllBytes(new File(args[1]).toPath()), "UTF-8");
        Object datum = new GenericDatumReader<Object>(s).read(null, DecoderFactory.get().jsonDecoder(s, json));
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        BinaryEncoder e = new EncoderFactory().configureBlockSize(Integer.parseInt(args[2])).blockingBinaryEncoder(bo, null);
        new GenericDatumWriter<Object>(s).write(datum, e);
        e.flush();
        System.out.write(bo.toByteArray()); System.out.flush();
    }
}
