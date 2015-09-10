package eu.dnetlib.iis.core.examples.javamapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
 * @author Mateusz Kobos
 */
public class PersonClonerMapper 
extends Mapper<AvroKey<Person>, NullWritable, AvroKey<String>, AvroValue<Person>> {
	
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
	        context.write(
	        		new AvroKey<String>(key.datum().getName().toString()), 
	        		new AvroValue<Person>(key.datum()));
		}
	}
}
