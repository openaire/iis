package eu.dnetlib.iis.workflows.top.importer.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.googlecode.protobuf.format.JsonFormat;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.iis.common.javamapreduce.hack.SchemaSetter;


/**
 * Input format returning Results built from JSON objects.
 * @author mhorst
 *
 */
public class PredefinedTableInputFormat extends InputFormat<ImmutableBytesWritable, Result> {

	public static final String IDX_PROPS_LOC = "import.input.format.idx.props.location";
	
	public static final String DEFAULT_CHARSET = "utf8";
	
	public static final String PART_SEPARATOR = "#";
	
	public static class FakeSplit extends InputSplit implements Writable {
	    public void write(DataOutput out) throws IOException { }
	    public void readFields(DataInput in) throws IOException { }
	    public long getLength() { return 0L; }
	    public String[] getLocations() { return new String[0]; }
	  }
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		List<InputSplit> ret = new ArrayList<InputSplit>();
		ret.add(new FakeSplit());
		return ret;
	}

	@Override
	public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
//		required by our hack related to multiple outputs
		SchemaSetter.set(context.getConfiguration());
		
		return new RecordReader<ImmutableBytesWritable, Result>() {
			
			private Iterator<Tuple> currentIt;
			private Tuple currentTuple;
			private float progress;
			
			class Tuple {
				final ImmutableBytesWritable key;
				final Result value;

				public Tuple (ImmutableBytesWritable key,
				Result value) {
					this.key = key;
					this.value = value;
				}
			}
			
			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				progress = 0;
				String idxLoc = context.getConfiguration().get(IDX_PROPS_LOC);
				if (idxLoc!=null) {
					List<Tuple> tuples = new ArrayList<Tuple>();
					Properties props = new Properties();
					InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(
			        		idxLoc);
					if (in==null) {
						throw new IOException("unable to load resource from path: " + idxLoc);
					}
					try {
						props.load(in);
				    	for (Map.Entry<Object, Object> entry : props.entrySet()) {
				    		System.err.println("key: " + entry.getKey() + ", value: " + entry.getValue());
				    		String id = loadId(entry.getKey() + File.separator + "id");
				    		System.err.println("id: " + id);
				    		byte[] idBytes = id.getBytes(DEFAULT_CHARSET);
				    		String[] parts = StringUtils.split(entry.getValue().toString(), ',');
				    		System.err.println("parts: " + Arrays.asList(parts));
				    		List<KeyValue> protoParts = new ArrayList<KeyValue>();
				    		for (String part : parts) {
				    			String[] familyWithQualifier = StringUtils.split(part, PART_SEPARATOR, 2);
				    			if (familyWithQualifier==null || familyWithQualifier.length!=2) {
				    				throw new RuntimeException("invalid part name value: '" + part + 
				    						"' not compliant with 'family" + 
				    						PART_SEPARATOR + "qualifier' template!");
				    			}
				    			Oaf.Builder oafBuilder = Oaf.newBuilder();
				    			String resourcePath = entry.getKey() + File.separator + part;
				    			String oafText = IOUtils.toString(
					    				Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath));
					    		try {
					    			JsonFormat.merge(oafText, oafBuilder);	
					    		} catch (Exception e) {
					    			throw new RuntimeException("got exception when parsing text resource: " +
					    					resourcePath + ", text content: " + oafText, e);
					    		}
				    			
					    		Oaf oaf = oafBuilder.build();
					    		KeyValue keyValue = new KeyValue(
					    				idBytes, 
					    				familyWithQualifier[0].getBytes(DEFAULT_CHARSET),
					    				familyWithQualifier[1].getBytes(DEFAULT_CHARSET),
					    				oaf.toByteArray());
					    		protoParts.add(keyValue);
					    		System.err.println(JsonFormat.printToString(oaf));	
				    		}
				    		tuples.add(new Tuple(
				    				new ImmutableBytesWritable(idBytes), 
				    				new Result(protoParts)));
				    	}
				    	currentIt = tuples.iterator();
					} finally {
						in.close();
					}
				} else {
					throw new IOException("no " + IDX_PROPS_LOC + " property set!");
				}

			}

		    private String loadId(String loc) throws IOException {
		    	return IOUtils.readLines(
		    			Thread.currentThread().getContextClassLoader().getResourceAsStream(loc)).iterator().next().trim();
		    }
			
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (currentIt.hasNext()) {
					currentTuple = currentIt.next();
					progress++;
					return true;
				} else {
					return false;
				}
			}

			@Override
			public ImmutableBytesWritable getCurrentKey() throws IOException,
					InterruptedException {
				return currentTuple!=null?currentTuple.key:null;
			}

			@Override
			public Result getCurrentValue() throws IOException,
					InterruptedException {
				return currentTuple!=null?currentTuple.value:null;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return progress;
			}

			@Override
			public void close() throws IOException {
				currentIt = null;
				currentTuple = null;
			}
		};
	}

}
