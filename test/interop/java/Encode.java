import org.apache.avro.Schema;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

// Usage: Encode schema.avsc datum.json -> prints the GenericDatumWriter bytes as hexadecimal.
public class Encode {
    public static void main(String[] args) throws Exception {
        Schema schema = new Schema.Parser().parse(new File(args[0]));
        String json = Files.readString(new File(args[1]).toPath(), StandardCharsets.UTF_8);
        byte[] bytes = CompareBytes.enc(schema, json);
        for (byte value : bytes) {
            System.out.printf("%02x", value & 0xff);
        }
        System.out.println();
    }
}
