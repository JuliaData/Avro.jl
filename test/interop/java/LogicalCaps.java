import org.apache.avro.*;
import java.io.*;
public class LogicalCaps {
    public static void main(String[] args) throws Exception {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        for (Schema.Field f : s.getFields()) {
            LogicalType lt = LogicalTypes.fromSchemaIgnoreInvalid(f.schema());
            Object conv = null;
            if (lt != null) conv = org.apache.avro.generic.GenericData.get().getConversionFor(lt);
            System.out.println(f.name() + "\t" + (lt == null ? "none" : lt.getName()) + "\t" + (conv == null ? "no-conversion" : conv.getClass().getSimpleName()));
        }
    }
}
