package eu.dnetlib.iis.common.javamapreduce.hack.oldapi;

import java.io.IOException;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Class to be used in Oozie map-reduce workflow node definition
 * @author Mateusz Kobos
 *
 */
public class InputFormat<T> extends AvroInputFormat<T> {
	@Override
	public RecordReader<AvroWrapper<T>, NullWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		SchemaSetter.set(job);
		return super.getRecordReader(split, job, reporter);
	}
}
