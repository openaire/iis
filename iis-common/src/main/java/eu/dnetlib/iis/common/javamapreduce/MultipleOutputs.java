package eu.dnetlib.iis.common.javamapreduce;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import eu.dnetlib.iis.common.javamapreduce.hack.AvroMultipleOutputs;

/**
 * Helper class for writing data to multiple outputs in accordance with our
 * conventions
 * 
 * @author Mateusz Kobos
 * 
 */
public class MultipleOutputs implements Closeable {
	private final AvroMultipleOutputs mos;

	public MultipleOutputs(TaskInputOutputContext<?, ?, ?, ?> context) {
		this.mos = new AvroMultipleOutputs(context);
		createOutputFiles(this.mos);
	}
	
	/** Create output files for each of named outputs by initializing writers
	 * assigned to each of them. This is a HACK.
	 * 
	 * Originally in the {@link AvroMultipleOutputs} class, in a situation when 
	 * there are no records in the input data, there were no output files
	 * generated. This is bad, because according to our conventions, a workflow
	 * node should always produce data stores it defines as its output, may they
	 * be filled with records or empty. Producing nothing at all complicates
	 * things and would have to be separately handled by a downstream workflow
	 * node which would introduce unneeded complexity.
	 * 
	 * The output records are created by writers associated with 
	 * each named output ("port" in the parlance of our conventions). A given output
	 * file is created during writer initialization. However, the initialization
	 * is done lazily in the {@code AvroMultipleOutputs.write} method, i.e. 
	 * the writer is initialized at the first attempt of writing something. If
	 * there are no input records, the map function of a mapper that writes
	 * something is not called even once and the initialization doesn't happen.
	 * 
	 * Here we're essentially undoing this lazy initialization by explicitly
	 * initializing the writers in the constructor.
	 */
	private static void createOutputFiles(AvroMultipleOutputs mos){
		for(String namedOutput: mos.getNamedOutputs()){
			TaskAttemptContext taskContext;
			try {
				taskContext = mos.getContext(namedOutput);
				mos.getRecordWriter(taskContext, getPortOutputPath(namedOutput));
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Write record embedded in {@code AvroKey} object to data store related to
	 * given port
	 */
	public <T> void write(String portName, AvroKey<T> record)
			throws IOException, InterruptedException {
		mos.write(portName, record, NullWritable.get(), 
				getPortOutputPath(portName));
	}
	
	private static String getPortOutputPath(String portName){
		return portName + "/part";
	}

	@Override
	public void close() throws IOException {
		try {
			this.mos.close();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public TaskAttemptContext getContext(String nameOutput) throws IOException {
		return this.mos.getContext(nameOutput);
	}

	@SuppressWarnings("rawtypes")
	public RecordWriter getRecordWriter(TaskAttemptContext taskContext, String baseFileName) throws IOException, InterruptedException{
		return this.mos.getRecordWriter(taskContext, baseFileName);
	}
}
