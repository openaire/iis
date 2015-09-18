package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Document;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/** Produce data stores
 * @author Mateusz Kobos
 */
public class SampleDataProducer implements Process {
	
	private final static String personPort = "person";
	private final static String documentPort = "document";
	
	public Map<String, PortType> getInputPorts() {
		return new HashMap<String, PortType>();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(personPort, 
				new AvroPortType(Person.SCHEMA$));
		outputPorts.put(documentPort, 
				new AvroPortType(Document.SCHEMA$));
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(conf);
		
		DataStore.create(StandardDataStoreExamples.getDocument(),
				new FileSystemPath(fs, output.get(documentPort)));
		DataStore.create(StandardDataStoreExamples.getPerson(),
				new FileSystemPath(fs, output.get(personPort)));
	}

}