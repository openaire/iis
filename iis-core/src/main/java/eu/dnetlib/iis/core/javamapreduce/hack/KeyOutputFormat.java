package eu.dnetlib.iis.core.javamapreduce.hack;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Class to be used in Oozie map-reduce workflow node definition
 * @author Mateusz Kobos
 */
public class KeyOutputFormat<T> extends AvroKeyOutputFormat<T> {
	@Override
	public RecordWriter<AvroKey<T>, NullWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException {
		SchemaSetter.set(context.getConfiguration());
		return super.getRecordWriter(context);
	}
}
