package eu.dnetlib.iis.core.javamapreduce.hack;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Class to be used in Oozie map-reduce workflow node definition
 * @author Mateusz Kobos
 */
public class KeyInputFormat<T> extends AvroKeyInputFormat<T> {
	@Override
	public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		SchemaSetter.set(context.getConfiguration());
		return super.createRecordReader(split, context);
	}
}
