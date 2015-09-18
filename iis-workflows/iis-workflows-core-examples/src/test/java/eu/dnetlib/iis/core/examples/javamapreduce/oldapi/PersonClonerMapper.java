package eu.dnetlib.iis.core.examples.javamapreduce.oldapi;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
 * @author Mateusz Kobos
 */
@Deprecated
public class PersonClonerMapper 
extends AvroMapper<Person, Pair<Utf8, Person>> {
	
	private final Utf8 emptyText = new Utf8("");
	private int copiesCount;
	
	/** This is the place you can access map-reduce workflow node parameters */
	@Override
	public void configure(JobConf jobConf) {
		copiesCount = Integer.parseInt(jobConf.get("copiesCount"));
	}
	
	@Override
	public void map(Person person,
			AvroCollector<Pair<Utf8, Person>> collector, Reporter reporter)
			throws IOException {
		for(int i = 0; i < copiesCount; i++){
	        collector.collect(new Pair<Utf8, Person>(emptyText, person));
		}
	}

}
