import org.apache.avro.*;
import java.nio.ByteBuffer;
public class DecEdge {
    public static void main(String[] args) throws Exception {
        Schema s = new Schema.Parser().parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}");
        Conversions.DecimalConversion c = new Conversions.DecimalConversion();
        byte[][] payloads = { {}, {0}, {(byte)0xff}, {0x30, 0x39}, {0x01, (byte)0x86, (byte)0xa0}, {0, 0, 0, 0x7b} };
        for (byte[] p : payloads) {
            try { System.out.println(java.util.Arrays.toString(p) + " -> " + c.fromBytes(ByteBuffer.wrap(p), s, s.getLogicalType())); }
            catch (Exception e) { System.out.println(java.util.Arrays.toString(p) + " -> ERROR " + e.getClass().getSimpleName() + ": " + e.getMessage()); }
        }
        Schema noscale = new Schema.Parser().parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4}");
        System.out.println("absent scale -> " + ((LogicalTypes.Decimal) noscale.getLogicalType()).getScale());
        try { System.out.println("toBytes 12345 (5 digits > precision 4): " + c.toBytes(new java.math.BigDecimal("123.45"), s, s.getLogicalType())); }
        catch (Exception e) { System.out.println("toBytes overflow -> ERROR " + e.getClass().getSimpleName() + ": " + e.getMessage()); }
    }
}
