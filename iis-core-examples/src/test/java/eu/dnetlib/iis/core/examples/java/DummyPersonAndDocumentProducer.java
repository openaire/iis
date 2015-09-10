package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Document;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/** Do not produce any output data. 
 * 
 * Optionally, it can produce empty directories instead of the true output 
 * data stores. Normally a workflow node should either create a true output
 * data store (possibly without any elements inside) or not create any output 
 * at all. Here, we're creating empty output directory exclusively for 
 * testing purposes,  i.e. this is not a recommended practice. 
 * @author Mateusz Kobos
 */
public class DummyPersonAndDocumentProducer implements Process {
	
	private final static String personPort = "person";
	private final static String documentPort = "document";
	private final static boolean defaultProduceEmptyDirs = false;
	private final static String produceEmptyDirsParam = "produceEmptyDirs";
	
	@Override
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
		outputPorts.put(personPort, new AvroPortType(Person.SCHEMA$));
		outputPorts.put(documentPort, new AvroPortType(Document.SCHEMA$));
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		boolean produceEmptyDirs = defaultProduceEmptyDirs;
		if(parameters.containsKey(produceEmptyDirsParam)){
			produceEmptyDirs = Boolean.parseBoolean(
					parameters.get(produceEmptyDirsParam));
		}
		if(produceEmptyDirs){
			FileSystem fs = FileSystem.get(conf);
			for(Map.Entry<String, Path> entry: portBindings.getOutput().entrySet()){
				fs.mkdirs(entry.getValue());
			}
		}
	}
}
