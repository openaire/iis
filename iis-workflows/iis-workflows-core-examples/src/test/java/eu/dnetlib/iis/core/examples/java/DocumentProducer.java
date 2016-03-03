package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Document;

/** Produce data store with documents 
 * @author Pawel Szostek 
 */
public class DocumentProducer implements Process {
	
	private final static String documentPort = "document";
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return new HashMap<String, PortType>();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = new HashMap<String, PortType>();
		outputPorts.put(documentPort, new AvroPortType(Document.SCHEMA$));
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration configuration,
			Map<String, String> parameters)	throws IOException{
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(configuration);
		
		DataStore.create(StandardDataStoreExamples.getDocument(),
				new FileSystemPath(fs, output.get(documentPort)));
	}

}