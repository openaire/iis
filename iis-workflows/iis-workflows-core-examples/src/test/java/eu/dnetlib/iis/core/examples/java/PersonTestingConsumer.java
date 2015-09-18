package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.TestsIOUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/**
 * Reads the Person data store and compares it with the sample Person data
 * store cloned a defined number of times. If There is a mismatch,
 * an exception is thrown.
 *
 * @author Mateusz Kobos
 */
public class PersonTestingConsumer implements Process {
	
	private final static String personPort = "person";
	private final static int defaultExpectedCopies = 1;
	private final static String expectedCopiesParam = "expected_copies";
	
	public Map<String, PortType> getInputPorts() {
		return createInputPorts();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return new HashMap<String, PortType>();
	}

	private static HashMap<String, PortType> createInputPorts(){
		HashMap<String, PortType> ports = new HashMap<String, PortType>();
		ports.put(personPort, new AvroPortType(Person.SCHEMA$));
		return ports;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		int expectedCopies = defaultExpectedCopies;
		if(parameters.containsKey(expectedCopiesParam)){
			expectedCopies = Integer.parseInt(parameters.get(expectedCopiesParam));
		}
		
		Map<String, Path> input = portBindings.getInput();
		FileSystem fs = FileSystem.get(conf);
		List<Person> actual = DataStore.read(
				new FileSystemPath(fs, input.get(personPort)));
		List<Person> expected = StandardDataStoreExamples.getPersonRepeated(
				expectedCopies);
		TestsIOUtils.assertEqualSets(expected, actual);
	}

}