import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
public class BigDec {
    public static void main(String[] args) throws Exception {
        Schema s = new Schema.Parser().parse("{\"type\":\"bytes\",\"logicalType\":\"big-decimal\"}");
        Conversions.BigDecimalConversion c = new Conversions.BigDecimalConversion();
        for (String v : args) {
            ByteBuffer bb = c.toBytes(new BigDecimal(v), s, LogicalTypes.fromSchema(s));
            byte[] b = new byte[bb.remaining()]; bb.get(b);
            StringBuilder sb = new StringBuilder();
            for (byte x : b) sb.append(String.format("%02x", x));
            System.out.println(v + "\t" + sb);
        }
    }
}
