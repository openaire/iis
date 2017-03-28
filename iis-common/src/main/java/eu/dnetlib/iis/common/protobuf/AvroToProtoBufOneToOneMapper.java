package eu.dnetlib.iis.common.protobuf;

import com.google.protobuf.Message;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class AvroToProtoBufOneToOneMapper<IN extends IndexedRecord, OUT extends Message>
        extends Mapper<AvroKey<IN>, NullWritable, Text, BytesWritable> {
    private static final String CONVERTER_CLASS_PROPERTY = "converter_class";
    private static final Logger log = Logger.getLogger(AvroToProtoBufOneToOneMapper.class);

    private final Text keyWritable = new Text();
    private final BytesWritable valueWritable = new BytesWritable();
    private AvroToProtoBufConverter<IN, OUT> converter;

    @SuppressWarnings("unchecked")
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Class<?> converterClass = context.getConfiguration().getClass(CONVERTER_CLASS_PROPERTY, null);

        if (converterClass == null) {
            throw new IOException("Please specify " + CONVERTER_CLASS_PROPERTY);
        }

        try {
            converter = (AvroToProtoBufConverter<IN, OUT>) converterClass.newInstance();
        } catch (ClassCastException e) {
            throw new IOException(
                    "Class specified in " + CONVERTER_CLASS_PROPERTY + " doesn't implement AvroToProtoBufConverter", e);
        } catch (Exception e) {
            throw new IOException(
                    "Could not instantiate specified AvroToProtoBufConverter class, " + converterClass, e);
        }
    }

    @Override
    public void map(AvroKey<IN> avro, NullWritable ignore, Context context)
            throws IOException, InterruptedException {
        String key = null;
        try {
            key = converter.convertIntoKey(avro.datum());
            keyWritable.set(key);

            byte[] value = converter.convertIntoValue(avro.datum()).toByteArray();
            valueWritable.set(value, 0, value.length);

            context.write(keyWritable, valueWritable);
        } catch (Exception e) {
            log.error("Error" + (key != null ? " while processing  " + key : ""), e);
        }
    }
}
