import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.*;
import java.io.File;
// Usage: TimeRead file.avro reps -> decodes all records to GenericRecord reps times; prints best wall time in ms and row count
public class TimeRead {
    public static void main(String[] args) throws Exception {
        File f = new File(args[0]); int reps = Integer.parseInt(args[1]);
        long best = Long.MAX_VALUE; long rows = 0;
        for (int i = 0; i < reps; i++) {
            long t0 = System.nanoTime(); rows = 0;
            try (DataFileReader<GenericRecord> r = new DataFileReader<>(f, new GenericDatumReader<GenericRecord>())) {
                GenericRecord rec = null;
                while (r.hasNext()) { rec = r.next(rec); rows++; }
            }
            best = Math.min(best, System.nanoTime() - t0);
        }
        System.out.println("rows=" + rows + " best_ms=" + (best / 1e6));
    }
}
