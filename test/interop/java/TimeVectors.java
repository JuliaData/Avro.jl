import org.apache.avro.*;
import org.apache.avro.data.TimeConversions;
import java.time.*;
public class TimeVectors {
    public static void main(String[] args) throws Exception {
        Instant[] instants = { Instant.parse("2000-01-01T10:00:00Z"), Instant.parse("1970-01-01T00:00:00Z"), Instant.parse("1969-12-31T23:59:59.999Z"),
            Instant.parse("2020-01-01T12:00:00.123456789Z"), Instant.parse("1600-02-29T00:00:00Z"), Instant.parse("2262-04-11T23:47:16.854775807Z") };
        Schema lms = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema lus = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema lns = LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
        Schema llms = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema llus = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema llns = LogicalTypes.localTimestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
        for (Instant i : instants) {
            System.out.println("timestamp-millis\t" + i + "\t" + new TimeConversions.TimestampMillisConversion().toLong(i, lms, lms.getLogicalType()));
            System.out.println("timestamp-micros\t" + i + "\t" + new TimeConversions.TimestampMicrosConversion().toLong(i, lus, lus.getLogicalType()));
            try { System.out.println("timestamp-nanos\t" + i + "\t" + new TimeConversions.TimestampNanosConversion().toLong(i, lns, lns.getLogicalType())); } catch (ArithmeticException ex) { System.out.println("timestamp-nanos\t" + i + "\tERROR:" + ex.getMessage()); }
            LocalDateTime l = LocalDateTime.ofInstant(i, ZoneOffset.UTC);
            System.out.println("local-timestamp-millis\t" + l + "\t" + new TimeConversions.LocalTimestampMillisConversion().toLong(l, llms, llms.getLogicalType()));
            System.out.println("local-timestamp-micros\t" + l + "\t" + new TimeConversions.LocalTimestampMicrosConversion().toLong(l, llus, llus.getLogicalType()));
            try { System.out.println("local-timestamp-nanos\t" + l + "\t" + new TimeConversions.LocalTimestampNanosConversion().toLong(l, llns, llns.getLogicalType())); } catch (ArithmeticException ex) { System.out.println("local-timestamp-nanos\t" + l + "\tERROR:" + ex.getMessage()); }
        }
        ZonedDateTime hel = ZonedDateTime.of(2000, 1, 1, 12, 0, 0, 0, ZoneId.of("Europe/Helsinki"));
        System.out.println("helsinki-timestamp-millis\t" + hel + "\t" + new TimeConversions.TimestampMillisConversion().toLong(hel.toInstant(), lms, lms.getLogicalType()));
        System.out.println("helsinki-local-timestamp-millis\t" + hel.toLocalDateTime() + "\t" + new TimeConversions.LocalTimestampMillisConversion().toLong(hel.toLocalDateTime(), llms, llms.getLogicalType()));
        Schema d = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        for (LocalDate ld : new LocalDate[]{ LocalDate.of(1970,1,1), LocalDate.of(1969,12,31), LocalDate.of(2020,2,29), LocalDate.of(1,1,1), LocalDate.of(9999,12,31) })
            System.out.println("date\t" + ld + "\t" + new TimeConversions.DateConversion().toInt(ld, d, d.getLogicalType()));
        Schema tm = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Schema tu = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        for (LocalTime lt : new LocalTime[]{ LocalTime.MIDNIGHT, LocalTime.of(12,34,56,123456789), LocalTime.MAX })
        { System.out.println("time-millis\t" + lt + "\t" + new TimeConversions.TimeMillisConversion().toInt(lt, tm, tm.getLogicalType()));
          System.out.println("time-micros\t" + lt + "\t" + new TimeConversions.TimeMicrosConversion().toLong(lt, tu, tu.getLogicalType())); }
    }
}
