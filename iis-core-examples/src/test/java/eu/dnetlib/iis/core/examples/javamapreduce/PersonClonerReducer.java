package eu.dnetlib.iis.core.examples.javamapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
 * @author Mateusz Kobos
 */
public class PersonClonerReducer 
extends Reducer<AvroKey<String>, AvroValue<Person>, AvroKey<Person>, NullWritable> {
	
	/**
	 * Copy the data without introducing any changes
	 */	
	@Override
	public void reduce(AvroKey<String> key, 
			Iterable<AvroValue<Person>> values, Context context)
	throws IOException, InterruptedException {
		CharSequence name = key.datum();
	    for(AvroValue<Person> value : values) {
	    	if(!name.equals(value.datum().getName())){
	    		throw new RuntimeException(
	    			"Person objects processed by the reducer do not have the same name! "+
	    			"I.e., "+name+" != "+value.datum().getName()+". "+
	    			"This is not possible, since the name should be the key of the reducer");
	    	}
	    	context.write(new AvroKey<Person>(value.datum()), 
	    			NullWritable.get());
	    }
	}
}
