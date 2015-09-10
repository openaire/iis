package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/** Create a certain number of copies of input records
 * @author Mateusz Kobos
 */
public class PersonCloner implements Process {
	
	private final static String personPort = "person";
	private final static int defaultCopies = 2;
	private final static String copiesParam = "copies";
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return createInputPorts();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createInputPorts(){
		HashMap<String, PortType> inputPorts = 
				new HashMap<String, PortType>();
		inputPorts.put(personPort, 
				new AvroPortType(Person.SCHEMA$));
		return inputPorts;
	}
	
	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(personPort, 
				new AvroPortType(Person.SCHEMA$));
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		Map<String, Path> input = portBindings.getInput();
		Map<String, Path> output = portBindings.getOutput();
		
		int copies = defaultCopies;
		if(parameters.containsKey(copiesParam)){
			copies = Integer.parseInt(parameters.get(copiesParam));
		}
		FileSystem fs = FileSystem.get(conf);
		
		List<Person> personsIn = DataStore.read(
				new FileSystemPath(fs, input.get(personPort))); 
		
		List<Person> personsOut = clone(personsIn, copies);

		DataStore.create(personsOut, 
				new FileSystemPath(fs, output.get(personPort)), 
				Person.SCHEMA$);
	}
	
	private static List<Person> clone(List<Person> persons, int copies){
		List<Person> list = new ArrayList<Person>();
		for(int i = 0; i < copies; i++){
			for(Person p: persons){
				list.add(Person.newBuilder()
						.setId(p.getId())
						.setName(p.getName())
						.setAge(p.getAge())
						.build());
			}
		}
		return list;
	}
}
