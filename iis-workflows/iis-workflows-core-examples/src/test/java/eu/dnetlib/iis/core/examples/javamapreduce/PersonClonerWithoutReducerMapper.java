package eu.dnetlib.iis.core.examples.javamapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
 * @author Mateusz Kobos
 */
public class PersonClonerWithoutReducerMapper 
extends Mapper<AvroKey<Person>, NullWritable, AvroKey<Person>, NullWritable> {
	
	private int copiesCount;
	
	/** This is the place where you can access map-reduce workflow node 
	 * parameters */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		copiesCount = Integer.parseInt(context.getConfiguration().get(
				"copiesCount"));
	}

	@Override
	protected void map(AvroKey<Person> key, NullWritable ignore, Context context)
					 throws IOException, InterruptedException{
		for (int i = 0; i < copiesCount; i++) {
	        context.write(new AvroKey<Person>(key.datum()),	
	        		NullWritable.get());
		}
	}
}
