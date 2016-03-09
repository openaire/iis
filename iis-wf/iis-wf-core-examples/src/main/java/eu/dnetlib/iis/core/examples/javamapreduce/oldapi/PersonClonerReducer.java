package eu.dnetlib.iis.core.examples.javamapreduce.oldapi;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.Reporter;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
* @author Mateusz Kobos
*/
@Deprecated
public class PersonClonerReducer extends AvroReducer<Utf8, Person, Person> {
	
	/**
	 * Copy the data without introducing any changes
	 */
	@Override
	public void reduce(Utf8 key, Iterable<Person> values, 
			AvroCollector<Person> collector, Reporter reporter)
	throws IOException{
		for(Person person: values){
			collector.collect(person);
		}
	}

}
