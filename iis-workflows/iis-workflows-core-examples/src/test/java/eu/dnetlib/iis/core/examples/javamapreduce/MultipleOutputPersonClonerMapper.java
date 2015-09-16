package eu.dnetlib.iis.core.examples.javamapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.PersonAge;
import eu.dnetlib.iis.core.javamapreduce.MultipleOutputs;

/**
 * Mapper that writes to multiple outputs.
 * Clones person objects and additionally writes age to the second output.
 * Number of objects written to both outputs are defined by copiesCount property.
 * @author mhorst
 * @author Mateusz Kobos
 *
 */
public class MultipleOutputPersonClonerMapper
	extends Mapper<AvroKey<Person>, NullWritable, NullWritable, NullWritable> {/*-?|2013-01-07 Cermine integration and MR multiple in-out|mafju|c3|?*/
	
	private int copiesCount;
	private MultipleOutputs mos;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.copiesCount = Integer.parseInt(
				context.getConfiguration().get("copiesCount"));
		this.mos = new MultipleOutputs(context);
	}
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		mos.close();
	}
	
	@Override
	public void map(AvroKey<Person> key, NullWritable ignore, Context context)
			throws IOException, InterruptedException {
		for(int i = 0; i < copiesCount; i++){
			mos.write("person", new AvroKey<Person>(key.datum()));
			PersonAge age = new PersonAge(key.datum().getAge());
			mos.write("age", new AvroKey<PersonAge>(age));
		}
	}

}
