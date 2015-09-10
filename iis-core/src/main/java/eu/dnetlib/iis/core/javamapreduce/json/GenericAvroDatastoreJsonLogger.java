package eu.dnetlib.iis.core.javamapreduce.json;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Generic avro datastore json logger.
 * @author mhorst
 *
 */
public class GenericAvroDatastoreJsonLogger extends Mapper<AvroKey<? extends SpecificRecordBase>, NullWritable, NullWritable, NullWritable> {

	private final static String maxLoggedElementsCount = "json.logger.max.count";
	
	private volatile int maxLoggedCount;
	
	private volatile int currentCount; 
	
	/** This is the place you can access map-reduce workflow node parameters */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		System.out.println("GenericAvroDatastoreJsonLogger#setup() call");
		maxLoggedCount = context.getConfiguration().getInt(maxLoggedElementsCount, 100);
		super.setup(context);
	}
	
	@Override
	protected void map(AvroKey<? extends SpecificRecordBase> key, NullWritable ignore, 
			Context context) throws IOException, InterruptedException {
		System.out.println("GenericAvroDatastoreJsonLogger#map() call");
		if (currentCount<maxLoggedCount) {
			if (key.datum()!=null) {
				EncoderFactory encoderFactory = new EncoderFactory();
				DatumWriter<Object> writer = new GenericDatumWriter<Object>(key.datum().getSchema());
				StringWriter strWriter = new StringWriter();
				try {
					JsonGenerator g = new JsonFactory().createJsonGenerator(strWriter);
					g.useDefaultPrettyPrinter();
					writer.write(key.datum(), encoderFactory.jsonEncoder(key.datum().getSchema(), g));
					g.flush();
					g.close();
					strWriter.flush();
					System.out.println("avro record " + currentCount + " content: ");
					System.out.println(strWriter.toString());
					currentCount++;
				} finally {
					strWriter.close();	
				}
			}	
		}
	}
}
