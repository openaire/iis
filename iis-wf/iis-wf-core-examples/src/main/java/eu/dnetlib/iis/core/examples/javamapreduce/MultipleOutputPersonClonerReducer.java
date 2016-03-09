package eu.dnetlib.iis.core.examples.javamapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.PersonAge;

/**
 * @author Mateusz Kobos
 */
public class MultipleOutputPersonClonerReducer extends
	Reducer<AvroKey<String>, AvroValue<Person>, AvroKey<Person>, NullWritable> {

	private MultipleOutputs mos;
	private int reducerCopiesCount;
	
	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs(context);
		reducerCopiesCount = Integer.parseInt(
				context.getConfiguration().get("reducerCopiesCount"));
	}
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		mos.close();
	}
	
	@Override
	public void reduce(AvroKey<String> key,
			Iterable<AvroValue<Person>> values, Context context)
			throws IOException, InterruptedException {
		CharSequence name = key.datum();
		for (AvroValue<Person> value : values) {
	    	if(!name.equals(value.datum().getName())){
	    		throw new RuntimeException(
	    			"Person objects processed by the reducer do not have the same name! "+
	    			"I.e., "+name+" != "+value.datum().getName()+". "+
	    			"This is not possible, since the name should be the key of the reducer");
	    	}
			for(int i = 0; i < reducerCopiesCount; i++){
				mos.write("person", new AvroKey<Person>(value.datum()));
				PersonAge age = new PersonAge(value.datum().getAge());
				mos.write("age", new AvroKey<PersonAge>(age));
			}
		}
	}
	
}
