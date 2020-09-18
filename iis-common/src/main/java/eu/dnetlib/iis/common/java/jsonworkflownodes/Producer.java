package eu.dnetlib.iis.common.java.jsonworkflownodes;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.common.java.jsonworkflownodes.PortSpecifications.SpecificationValues;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * The produced data stores are taken from resources in JSON format.
 * Names of output ports, their number, types, and produced data stores
 * depends on configuration passed through the constructor.
 * @author Mateusz Kobos
 *
 */
public class Producer implements Process {
	
	private final PortSpecifications outSpecs;
	
	/**
	 * @param outputSpecifications specification of output conforming to
	 * the following template:
	 * "{output port name, schema reference, path to JSON file in resources corresponding to output data store},
	 * e.g. "{person, eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person, eu/dnetlib/iis/core/examples/person.json}"
	 */
	public Producer(String[] outputSpecifications){
		outSpecs = new PortSpecifications(outputSpecifications);
	}

	@Override
	public Map<String, PortType> getInputPorts() {
		return new HashMap<String, PortType>();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return outSpecs.getPortTypes();
	}

	@Override
	public void run(PortBindings portBindings, Configuration configuration,
			Map<String, String> parameters) throws Exception {
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(configuration);
		for(Map.Entry<String, Path> e: output.entrySet()){
			SpecificationValues specs = outSpecs.get(e.getKey());
			try {
				write(new FileSystemPath(fs, e.getValue()), specs);	
			} catch (Exception ex) {
				throw new Exception("got exception when processing file " + 
						e.getValue().toString(), ex);
			}
		}
	}
	
	private static void write(FileSystemPath destination,
			SpecificationValues specs) throws IOException{
		InputStream in = StaticResourceProvider.getResourceInputStream(specs.getJsonFilePath());
		JsonUtils.convertToDataStore(specs.getSchema(), in, destination);
	}
}