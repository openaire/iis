package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/** Processes input files in a specific way
 * @author Mateusz Kobos
 */
public class TextDataProducer implements Process {
	
	private final static String personPort = "person";
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
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(personPort, new AnyPortType());
		outputPorts.put(documentPort, new AnyPortType());
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(conf);
		
		final String resourceDir = 
				"eu/dnetlib/iis/core/examples/simple_csv_data";
		copyResourceToHDFS(fs, resourceDir+"/person.csv", output.get(personPort));
		copyResourceToHDFS(fs, resourceDir+"/document.csv", output.get(documentPort));
	}
	
	public static void copyResourceToHDFS(
			FileSystem fs, String resourcePath, Path destination)
					throws IOException {
		InputStream in = TextDataProducer.class.getResourceAsStream(resourcePath);
		OutputStream out = fs.create(destination);
		IOUtils.copy(in, out);
	}

}
